"""Publish a snapshot of the dashboard's tables from the warehouse to app/data/*.parquet.

The public deployment (Streamlit Community Cloud) has no database, so it reads
these files instead. Re-run after each pipeline run to refresh the public demo:

    docker compose run --rm -v ./app/data:/app/data dashboard python export_data.py
"""

import os
from pathlib import Path

import pandas as pd
import psycopg

OUT = Path(__file__).resolve().parent / "data"

# Only the columns the dashboard uses; numerics as float8 to keep the files small and simple.
QUERIES = {
    "mart_hospital_value": """
        SELECT data_year, ccn, hospital_name, city, state, drg_cd, drg_desc, discharges,
               avg_total_payment::float8 AS avg_total_payment,
               avg_medicare_payment::float8 AS avg_medicare_payment,
               price_index_vs_state::float8 AS price_index_vs_state,
               overall_rating, readmission_rate_pct::float8 AS readmission_rate_pct, value_segment
        FROM marts.mart_hospital_value
    """,
    "mart_drg_price_trend": """
        SELECT data_year, drg_cd, drg_desc, discharges,
               weighted_avg_total_payment::float8 AS weighted_avg_total_payment,
               yoy_change_pct::float8 AS yoy_change_pct
        FROM marts.mart_drg_price_trend
    """,
}


def conninfo() -> str:
    return os.environ.get("WAREHOUSE_DSN") or (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} port={os.environ.get('WAREHOUSE_PORT', '5432')} "
        f"dbname={os.environ.get('WAREHOUSE_DB', 'warehouse')} user={os.environ.get('WAREHOUSE_USER', 'warehouse')} "
        f"password={os.environ.get('WAREHOUSE_PASSWORD', 'warehouse')}"
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(conninfo()) as conn:
        for name, sql in QUERIES.items():
            with conn.cursor() as cur:
                cur.execute(sql)
                df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
            path = OUT / f"{name}.parquet"
            df.to_parquet(path, index=False, compression="zstd")
            print(f"{path.name}: {len(df):,} rows, {path.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
