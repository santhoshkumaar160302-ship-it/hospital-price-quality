"""End-to-end test on fixture data (used by CI and `make e2e`).

Simulates two quarterly CMS releases arriving one after the other:
  1. reset the warehouse
  2. ingest phase 1 -> dbt build
  3. re-ingest phase 1 -> every file must be skipped (idempotency)
  4. ingest phase 2 -> dbt build (incremental fact + snapshot history)
  5. assert the results

Needs WAREHOUSE_* (or WAREHOUSE_DSN) env vars pointing at an empty-able Postgres
database, and dbt on PATH (or DBT_BIN).
"""

import os
import subprocess
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ingest.config import warehouse_conninfo  # noqa: E402
from ingest.pipeline import run_all  # noqa: E402

DBT = os.environ.get("DBT_BIN", "dbt")
FIXTURES = ROOT / "tests" / "fixtures"


def dbt(*args: str) -> None:
    cmd = [DBT, *args, "--profiles-dir", ".", "--project-dir", "."]
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT / "dbt", check=True)


def scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


def check(label: str, ok: bool, detail="") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {label} {detail}")
    if not ok:
        raise SystemExit(1)


def main() -> None:
    with psycopg.connect(warehouse_conninfo(), autocommit=True) as conn:
        conn.execute((ROOT / "scripts" / "reset_warehouse.sql").read_text())

    print("== phase 1: first release ==")
    r1 = run_all(source_dir=FIXTURES / "phase1", min_rows=1)
    check("phase 1 loaded all 4 datasets", sum(r["status"] == "loaded" for r in r1) == 4)
    dbt("build")

    print("== re-run phase 1: nothing new ==")
    again = run_all(source_dir=FIXTURES / "phase1", min_rows=1)
    check("same files are skipped on re-run", all(r["status"] == "skipped" for r in again))

    print("== phase 2: next release ==")
    run_all(source_dir=FIXTURES / "phase2", min_rows=1)
    dbt("build")

    with psycopg.connect(warehouse_conninfo()) as conn:
        years = conn.execute(
            "SELECT data_year, count(*) FROM marts.fct_inpatient_drg_payments GROUP BY 1 ORDER BY 1").fetchall()
        check("incremental fact has both data years", [y for y, _ in years] == [2023, 2024], years)

        versions = conn.execute(
            "SELECT overall_rating FROM marts.dim_hospital_history WHERE ccn = '050003' ORDER BY version").fetchall()
        check("SCD2 kept the old and new star rating", [v for (v,) in versions] == [2, 3], versions)

        check("hospital added in phase 2 is current",
              scalar(conn, "SELECT count(*) FROM marts.dim_hospital WHERE ccn = '450005'") == 1)
        check("stripped leading zero restored (50001 -> 050001)",
              scalar(conn, "SELECT count(*) FROM marts.dim_hospital WHERE ccn = '050001'") == 1)
        check("old column name Rndrng_CCN mapped for 2023",
              scalar(conn, "SELECT count(*) FROM marts.fct_inpatient_drg_payments WHERE data_year = 2023") > 0)
        check("new source column added to raw table",
              scalar(conn, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'raw' "
                           "AND table_name = 'hospital_general_info' AND column_name = 'telehealth_services'") == 1)
        check("no failed loads in audit",
              scalar(conn, "SELECT count(*) FROM raw._load_audit WHERE status <> 'success'") == 0)
        check("value mart populated",
              scalar(conn, "SELECT count(*) FROM marts.mart_hospital_value") > 0)
    print("E2E PASSED")


if __name__ == "__main__":
    main()
