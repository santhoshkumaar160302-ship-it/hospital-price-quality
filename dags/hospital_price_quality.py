"""Weekly pipeline: CMS hospital data -> raw -> dbt (staging, snapshot, marts, tests).

    ingest.<dataset> x4  (parallel, retried with backoff)
          │
    check_for_new_data   (skips everything below when no dataset had a new release)
          │
    dbt_source_freshness -> dbt_build (models + snapshot + tests in DAG order) -> dbt_docs

Most weeks CMS hasn't published anything new: every ingest task sees the same
file it loaded before (same release + sha256), skips, and the dbt steps are
skipped too. Trigger with {"force_reload": true} to reload and rebuild anyway.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

DATASETS = ["hospital_general", "unplanned_visits", "complications_deaths", "inpatient_drg"]
DBT = "cd /opt/airflow/dbt && /home/airflow/dbt-venv/bin/dbt {cmd} --profiles-dir . --project-dir ."

log = logging.getLogger(__name__)


def notify_failure(context) -> None:
    """Hook for alerting (Slack, email, PagerDuty). Logs for now."""
    ti = context["task_instance"]
    log.error("ALERT: %s.%s failed (try %s). Logs: %s", ti.dag_id, ti.task_id, ti.try_number, ti.log_url)


def ingest_dataset(dataset: str, **context) -> list[dict]:
    from ingest.pipeline import run_dataset  # imported at run time to keep DAG parsing fast

    return run_dataset(dataset, force=context["params"]["force_reload"])


def has_new_data(**context) -> bool:
    if context["params"]["force_reload"]:
        return True
    ti = context["ti"]
    results = ti.xcom_pull(task_ids=[f"ingest.{d}" for d in DATASETS]) or []
    loaded = [r for per_task in results if per_task for r in per_task if r["status"] == "loaded"]
    log.info("new releases loaded this run: %s", loaded or "none")
    return bool(loaded)


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="hospital_price_quality",
    description="CMS hospital price & quality: ingest, transform and test",
    schedule="0 7 * * 1",  # Mondays 07:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"force_reload": Param(False, type="boolean", description="Reload all files and rebuild")},
    tags=["cms", "healthcare", "dbt"],
    doc_md=__doc__,
) as dag:
    with TaskGroup("ingest") as ingest:
        for name in DATASETS:
            PythonOperator(
                task_id=name,
                python_callable=ingest_dataset,
                op_kwargs={"dataset": name},
            )

    check = ShortCircuitOperator(task_id="check_for_new_data", python_callable=has_new_data)

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT.format(cmd="source freshness"),
        retries=0,
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(cmd="build"),
        retries=1,
    )
    docs = BashOperator(
        task_id="dbt_docs",
        bash_command=DBT.format(cmd="docs generate"),
        retries=0,
    )

    ingest >> check >> freshness >> build >> docs
