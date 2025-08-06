from __future__ import annotations # Ensure compatibility with Python 3.7+
# Standard library imports
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import polars as pl 

from src.extract import load_config 

# Constants for the transform script
CFG = load_config()
ACTIVE_DAYS = CFG.get("active_days", 180)  # Default to 180 days if not specified
HERE = Path(__file__).resolve().parent

def _to_polars(df: List[Dict[str, Any]]) -> pl.DataFrame:
    """"
        Convert a list of dictionaries to a Polars DataFrame,
        handling datetime and numeric fields appropriately,
        ensuring compatibility with Polars' data types, this is because Github API returns data in a format that may not directly map to Polars types,
        sometimes inteerpreting datetime as string and numeric fields as strings.
    """
    if not df:
        return pl.DataFrame()  # Return an empty DataFrame if input is empty

    df = pl.DataFrame(df)

    # Convert datetime fields to Polars datetime type
    time_columns = [
        "created_at", "updated_at", "pushed_at"]
    for col in time_columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
            )
    
    # Cast explicitly numeric columns 
    numeric_columns = [
        "id", "stargazers_count", "watchers_count", "size", "open_issues_count", "fork_count" ]
    for col in numeric_columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Int64)
            )
    
    df.describe() # Debugging: print schema and sample data
    return df 

def _add_derived_columns(df : pl.DataFrame) -> pl.DataFrame:
    """"
        Add derived columns to the Dataframe such as:
        - days_since_last_push: Number of days since the last push
        - star_fork_ratio: Ratio of stars to forks
    """
    now = datetime.now(timezone.utc)
    df = df.with_columns(
        [
            (now - pl.col("pushed_at")).dt.days.alias("days_since_last_push"),
            (pl.col("stargazers_count")/pl.col("fork_count")).alias("star_fork_ratio")
        ]
    )
    return df

def _filter_active_repos(df: pl.DataFrame) -> pl.DataFrame:
    """
        Filter the DataFrame to include only active repositories based on the last push date.
    """
    return df.filter(pl.col("days_since_last_push") <= ACTIVE_DAYS)

def transform(df: List[Dict[str, Any]]) -> pl.DataFrame:
    """
        Transform the input data by converting it to a Polars DataFrame,
        adding derived columns, and filtering for active repositories.
    """
    df_polars = _to_polars(df)
    df_polars = _add_derived_columns(df_polars)
    df_polars = _filter_active_repos(df_polars)
    
    return df_polars
