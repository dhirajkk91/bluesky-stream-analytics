"""
### Rebuild marts

Rebuilds the marts for a range of past days, for when a fix to a model or a
late load should reach days that were already built. Trigger it with the first
and last day to rebuild.

Each day is a run of the marts unit for that day, one at a time. Some marts are
rebuilt in full on every run, and a run for a past day builds them only up to
that day, so a final build for today brings them back up to date before the
report's numbers are exported.
"""

from datetime import date, timedelta

import pendulum
from airflow.sdk import DAG, Param, task

from systemd_unit import SystemdUnitOperator

MAX_DAYS = 62


def days_to_rebuild(first_day: str, last_day: str) -> list[str]:
    """
    The marts units to start, one per day. A unit for day D builds the days
    before D, so rebuilding a day means running the unit for the day after it.
    """
    first, last = date.fromisoformat(first_day), date.fromisoformat(last_day)
    if last < first:
        raise ValueError(f"last_day {last_day} is before first_day {first_day}")
    if (last - first).days + 1 > MAX_DAYS:
        raise ValueError(f"at most {MAX_DAYS} days at a time")
    return [
        f"bluesky-marts@{first + timedelta(days=offset + 1)}.service"
        for offset in range((last - first).days + 1)
    ]


with DAG(
    dag_id="rebuild_marts",
    description="Rebuild the marts for a range of past days",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "first_day": Param(type="string", format="date", description="First day to rebuild"),
        "last_day": Param(type="string", format="date", description="Last day to rebuild"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bluesky"],
    doc_md=__doc__,
) as dag:

    @task
    def units(params=None) -> list[str]:
        return days_to_rebuild(params["first_day"], params["last_day"])

    rebuild = SystemdUnitOperator.partial(
        task_id="rebuild_day",
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=1),
    ).expand(unit=units())

    bring_up_to_date = SystemdUnitOperator(
        task_id="build_today",
        unit="bluesky-marts@{{ dag_run.run_after.strftime('%Y-%m-%d') }}.service",
        execution_timeout=timedelta(hours=1),
    )
    export = SystemdUnitOperator(task_id="export", unit="bluesky-export.service", retries=2)

    rebuild >> bring_up_to_date >> export
