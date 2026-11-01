"""
### Hourly

Loads the hour the uploader handed over into the event store, folds the new
events into the projections, then looks up the handles of accounts whose
identity changed. Each step starts only when the one before it has succeeded.

Every step reads whatever is new since its own last run, so a missed or failed
run is caught up by the next one. There's nothing to backfill, so catchup is off.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG

from systemd_unit import SystemdUnitOperator

with DAG(
    dag_id="hourly",
    description="Event store, projections and handle lookups",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        # Under an hour, so a stuck run gives way to the next one. A run can wait
        # for the lake lock while compaction holds it.
        "execution_timeout": timedelta(minutes=50),
    },
    tags=["bluesky"],
    doc_md=__doc__,
) as dag:
    process = SystemdUnitOperator(task_id="process", unit="bluesky-process.service")
    project = SystemdUnitOperator(task_id="project", unit="bluesky-project.service")
    resolve_handles = SystemdUnitOperator(task_id="resolve_handles", unit="bluesky-handles.service")

    process >> project >> resolve_handles
