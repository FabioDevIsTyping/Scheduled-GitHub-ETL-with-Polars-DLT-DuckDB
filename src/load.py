"""
Write the Polars DataFrame returned by ``transform.py`` into a DuckDB
file-based database.  The module is schema‑aware: it will create the
target table on first run, evolve it if new columns appear, and enforce
``id`` as a PRIMARY KEY so repeated daily runs with ``mode='append'`` do
not create duplicates.

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from src.extract import load_config

# Constants
CFG = load_config()
DUCKDB_PATH: str = os.getenv("DB_PATH", CFG.get("duckdb_path", "data/output.duckdb"))
TABLE_NAME: str = CFG.get("table_name", "repos")
WRITE_MODE: str = CFG.get("mode", "append").lower()  # "append" | "overwrite"

# Ensure the parent directory exists
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

# Private helpers

def _get_connection(db_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection: 
    """Open (or create) the DuckDB file and return a connection."""
    return duckdb.connect(database=db_path, read_only=False)


def _prepare_table(conn: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """Create table with primary key on ``id`` if it doesn't exist.

    The schema is inferred from the Polars DataFrame's Arrow schema.
    """
    existing = conn.execute(
        """SELECT COUNT(*)>0 AS has_table
               FROM information_schema.tables
              WHERE table_name = ?""",
        [table],
    ).fetchone()[0]

    if existing:
        return

    # Create table based on Polars -> Arrow schema
    arrow_schema = df.schema_arrow
    cols_sql = []
    for field in arrow_schema:
        name = field.name
        dtype = field.type.to_pandas_dtype()
        duck_type = {
            "int64": "BIGINT",
            "float64": "DOUBLE",
            "object": "VARCHAR",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }.get(str(dtype), "VARCHAR")
        cols_sql.append(f"{name} {duck_type}")

    # id is unique numeric identifier from GitHub; enforce PRIMARY KEY
    if "id" in df.columns:
        for i, col_sql in enumerate(cols_sql):
            if col_sql.startswith("id "):
                cols_sql[i] = col_sql + " PRIMARY KEY"
                break

    create_sql = f"CREATE TABLE {table} ({', '.join(cols_sql)});"
    conn.execute(create_sql)


def _add_missing_columns(conn: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:  # noqa: N802
    """If new columns appear in *df*, ALTER TABLE to add them."""
    existing_cols = {
        r[0] for r in conn.execute(
            """SELECT column_name FROM information_schema.columns
                    WHERE table_name = ?""",
            [table],
        ).fetchall()
    }
    arrow_schema = df.schema_arrow
    for field in arrow_schema:
        if field.name in existing_cols:
            continue
        dtype = field.type.to_pandas_dtype()
        duck_type = {
            "int64": "BIGINT",
            "float64": "DOUBLE",
            "object": "VARCHAR",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }.get(str(dtype), "VARCHAR")
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {field.name} {duck_type};")


# Public Functions
def write_frame(
    df: pl.DataFrame,
    db_path: str = DUCKDB_PATH,
    table_name: str = TABLE_NAME,
    mode: str = WRITE_MODE,
) -> None:
    """Write the Polars DataFrame *df* to a DuckDB table.
    *db_path* is the DuckDB file path, *table_name* is the target table,
    and *mode* is either 'append' or 'overwrite'.
    """
    if df.is_empty():
        print("[load] DataFrame is empty – nothing to write.")
        return

    mode = mode.lower()
    if mode not in {"append", "overwrite"}:
        raise ValueError("mode must be 'append' or 'overwrite'")

    with _get_connection(db_path) as conn:
        if mode == "overwrite":
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

        _prepare_table(conn, table_name, df)
        _add_missing_columns(conn, table_name, df)

        # Bulk insert via Arrow zero‑copy
        conn.register("incoming", df.to_arrow())
        conn.execute(
            f"INSERT OR IGNORE INTO {table_name} SELECT * FROM incoming;"
        )
        conn.unregister("incoming")

        conn.commit()
        print(f"[load] Wrote {len(df)} rows to {db_path}::{table_name} (mode={mode}).")


def load(df: pl.DataFrame, cfg: dict[str, Any] | None = None) -> None:
    """Facade used by ``main.py``.

    *cfg* can override ``duckdb_path``, ``table_name`` and ``mode`` at call time.
    """
    if cfg is None:
        cfg = {}

    write_frame(
        df=df,
        db_path=cfg.get("duckdb_path", DUCKDB_PATH),
        table_name=cfg.get("table_name", TABLE_NAME),
        mode=cfg.get("mode", WRITE_MODE),
    )


def preview(db_path: str = DUCKDB_PATH, table_name: str = TABLE_NAME, n: int = 5) -> pl.DataFrame:
    """Return the first *n* rows of *table_name* as a Polars DataFrame."""
    with _get_connection(db_path) as conn:
        arrow_tbl = conn.execute(
            f"SELECT * FROM {table_name} LIMIT {n};"
        ).fetch_arrow_table()
        return pl.from_arrow(arrow_tbl)

# Usage example:
if __name__ == "__main__":
    from src.extract import fetch_all
    from src.transform import transform

    rows = fetch_all()
    tidy = transform(rows)
    load(tidy)
    print(preview())
