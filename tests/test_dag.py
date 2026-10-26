"""DAG integrity test. Runs in CI where Airflow is installed; skipped otherwise."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("hospital_price_quality")


def test_dag_structure(dag):
    ids = set(dag.task_ids)
    assert {f"ingest.{d}" for d in ("hospital_general", "unplanned_visits", "complications_deaths",
                                    "inpatient_drg")} <= ids
    assert dag.get_task("check_for_new_data").upstream_task_ids == {
        t for t in ids if t.startswith("ingest.")}
    assert dag.get_task("dbt_build").upstream_task_ids == {"dbt_source_freshness"}
    assert dag.catchup is False and dag.max_active_runs == 1


def test_ingest_tasks_retry(dag):
    for t in dag.tasks:
        if t.task_id.startswith("ingest."):
            assert t.retries >= 3 and t.retry_exponential_backoff
