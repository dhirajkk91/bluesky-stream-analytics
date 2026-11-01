"""
### Nightly

Compaction first: it redacts deleted records in the event store and rewrites
the projections. Then the marts are built and tested with dbt, and the public
report's numbers exported. The vacuum, which deletes the files compaction
replaced, waits until compaction's snapshots are past the tables' one-hour
retention, so it can actually remove them.

The marts unit takes the day the build runs for, so a past day can be rebuilt
with the `rebuild_marts` DAG.
"""

from datetime import UTC, datetime, timedelta

import pendulum
from airflow.sdk import DAG, task

from systemd_unit import SystemdUnitOperator

# Tables keep an hour of snapshots; a little more makes sure compaction's have expired.
SNAPSHOT_WAIT = timedelta(minutes=70)


def build_end(logical_date, dag_run) -> str:
    """
    The day the marts are built up to (not including), as yyyy-mm-dd: the run's
    date. A manual run without a logical date uses when it was triggered.
    """
    moment = logical_date or dag_run.run_after
    return moment.strftime("%Y-%m-%d")


def snapshots_expired(compaction_finished: str, now: datetime) -> bool:
    return now >= datetime.fromisoformat(compaction_finished) + SNAPSHOT_WAIT


with DAG(
    dag_id="nightly",
    description="Compaction, marts, report export and vacuum",
    schedule="45 3 * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"build_end": build_end},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bluesky"],
    doc_md=__doc__,
) as dag:
    compact = SystemdUnitOperator(
        task_id="compact",
        unit="bluesky-compact.service",
        # The monthly run reaches every table in full, and may wait for the lake lock.
        execution_timeout=timedelta(hours=3),
    )
    build_marts = SystemdUnitOperator(
        task_id="build_marts",
        unit="bluesky-marts@{{ build_end(logical_date, dag_run) }}.service",
        execution_timeout=timedelta(hours=1),
    )
    export = SystemdUnitOperator(
        task_id="export",
        unit="bluesky-export.service",
        retries=2,
        execution_timeout=timedelta(minutes=20),
    )

    @task.sensor(task_id="wait_for_snapshots", poke_interval=300, timeout=3 * 3600, mode="reschedule")
    def wait_for_snapshots(compaction_finished: str) -> bool:
        return snapshots_expired(compaction_finished, datetime.now(UTC))

    vacuum = SystemdUnitOperator(
        task_id="vacuum",
        unit="bluesky-vacuum.service",
        execution_timeout=timedelta(hours=1),
    )

    compact >> build_marts >> export
    wait_for_snapshots(compact.output) >> vacuum
