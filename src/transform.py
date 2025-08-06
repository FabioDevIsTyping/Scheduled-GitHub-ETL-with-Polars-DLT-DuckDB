from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, UTC

import polars as pl

from src.extract import load_config, fetch_all

# Configuration
# Load configuration settings
CFG          = load_config()
ACTIVE_DAYS  = int(CFG.get("active_days", 180))
HERE         = Path(__file__).resolve().parent

#  Helpers 
def _to_pl(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Convert list-of-dict rows to Polars and enforce dtypes.
    """
    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)

    # Datetimes
    for col in ("created_at", "updated_at", "pushed_at"):
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
            )

    # Numerics
    for col in (
        "id", "stargazers_count", "forks_count",
        "size", "open_issues_count"
    ):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64))

    return df


def _add_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add derived analytic columns.
    - days_since_last_push: days since the last push
    - star_fork_ratio: ratio of stars to forks
    """
    now = datetime.utcnow()       

    df = df.with_columns([
        # Calculate days since last push
        (pl.lit(now) - pl.col("pushed_at")).dt.total_days()
          .alias("days_since_last_push"),

        (pl.when(pl.col("forks_count") > 0)
             .then(pl.col("stargazers_count") / pl.col("forks_count"))
             .otherwise(None)
             .alias("star_fork_ratio"))
    ])
    return df


def _filter_active(df: pl.DataFrame, active_days: int) -> pl.DataFrame:
    """
    Filter repositories that have been active in the last `active_days`.
    """
    return df.filter(pl.col("days_since_last_push") <= active_days)

# Orchestrator
def transform(rows: List[Dict[str, Any]],
              active_days: int | None = None) -> pl.DataFrame:
    """
    Convert raw rows into a tidy Polars DataFrame.
    """
    active_days = active_days or ACTIVE_DAYS
    df = _to_pl(rows)
    df = _add_metrics(df)
    df = _filter_active(df, active_days)
    return df

# Script mode
if __name__ == "__main__":
    data = fetch_all()
    tidy = transform(data)
    print(tidy.head())
    print(f"\nRows after filter: {len(tidy)}")
