import subprocess
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk.exceptions import AirflowException

from conftest import DAGS
from nightly import build_end, snapshots_expired
from rebuild_marts import MAX_DAYS, days_to_rebuild
from systemd_unit import UNIT_COMMAND, SystemdUnitOperator


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS))


def downstream(dag) -> dict[str, list[str]]:
    return {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}


def test_the_dags_load_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert sorted(dagbag.dag_ids) == ["hourly", "nightly", "rebuild_marts"]


def test_hourly_runs_its_steps_in_order(dagbag):
    dag = dagbag.get_dag("hourly")
    assert dag.timetable.expression == "20 * * * *"
    assert not dag.catchup and dag.max_active_runs == 1
    assert downstream(dag) == {"process": ["project"], "project": ["resolve_handles"], "resolve_handles": []}
    assert [dag.get_task(t).unit for t in ("process", "project", "resolve_handles")] == [
        "bluesky-process.service",
        "bluesky-project.service",
        "bluesky-handles.service",
    ]


def test_nightly_builds_the_marts_after_compaction_and_vacuums_once_snapshots_expire(dagbag):
    dag = dagbag.get_dag("nightly")
    assert dag.timetable.expression == "45 3 * * *"
    assert downstream(dag) == {
        "compact": ["build_marts", "wait_for_snapshots"],
        "build_marts": ["export"],
        "export": [],
        "wait_for_snapshots": ["vacuum"],
        "vacuum": [],
    }
    assert dag.get_task("build_marts").unit == "bluesky-marts@{{ build_end(logical_date, dag_run) }}.service"


def test_rebuild_marts_rebuilds_each_day_then_today_then_exports(dagbag):
    dag = dagbag.get_dag("rebuild_marts")
    assert type(dag.timetable).__name__ == "NullTimetable"  # only runs when triggered
    assert downstream(dag) == {
        "units": ["rebuild_day"],
        "rebuild_day": ["build_today"],
        "build_today": ["export"],
        "export": [],
    }
    assert sorted(dag.params.keys()) == ["first_day", "last_day"]


def test_the_marts_are_built_up_to_the_runs_day():
    run = SimpleNamespace(run_after=datetime(2026, 10, 31, 9, 12, tzinfo=UTC))
    assert build_end(datetime(2026, 10, 31, 3, 45, tzinfo=UTC), run) == "2026-10-31"
    # A manual run in Airflow 3 can have no logical date.
    assert build_end(None, run) == "2026-10-31"


def test_the_vacuum_waits_until_compactions_snapshots_have_expired():
    finished = "2026-10-31T03:52:00+00:00"
    assert not snapshots_expired(finished, datetime(2026, 10, 31, 4, 52, tzinfo=UTC))
    assert snapshots_expired(finished, datetime(2026, 10, 31, 5, 2, tzinfo=UTC))


def test_rebuilding_a_day_runs_the_unit_for_the_day_after_it():
    assert days_to_rebuild("2026-10-25", "2026-10-27") == [
        "bluesky-marts@2026-10-26.service",
        "bluesky-marts@2026-10-27.service",
        "bluesky-marts@2026-10-28.service",
    ]
    assert days_to_rebuild("2026-10-30", "2026-10-30") == ["bluesky-marts@2026-10-31.service"]
    with pytest.raises(ValueError, match="before"):
        days_to_rebuild("2026-10-27", "2026-10-25")
    last = (datetime(2026, 10, 1) + timedelta(days=MAX_DAYS)).strftime("%Y-%m-%d")
    with pytest.raises(ValueError, match="at most"):
        days_to_rebuild("2026-10-01", last)


class RecordingLog:
    def __init__(self):
        self.lines: list[str] = []

    def info(self, message, *args):
        self.lines.append(message % args if args else message)

    warning = info


@pytest.fixture
def task_log(monkeypatch):
    log = RecordingLog()
    monkeypatch.setattr(SystemdUnitOperator, "log", property(lambda self: log))
    return log


class FakeSystem:
    """Records the commands the operator runs, and answers like systemd would."""

    def __init__(self, fail: bool = False):
        self.calls: list[list[str]] = []
        self.fail = fail

    def __call__(self, args):
        args = list(args)
        self.calls.append(args)
        if args[0] == "sudo" and self.fail and "start" in args:
            return subprocess.CompletedProcess(args, 1, "", "Job for bluesky-process.service failed.\n")
        if args[0] == "systemctl":
            return subprocess.CompletedProcess(args, 0, "4f2c9a\n", "")
        if args[0] == "journalctl":
            return subprocess.CompletedProcess(args, 0, "Loaded 1,592,658 events\nCommitted\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")


def test_the_operator_starts_the_unit_and_copies_its_log(task_log):
    system = FakeSystem()
    op = SystemdUnitOperator(task_id="process", unit="bluesky-process.service", runner=system)
    finished = op.execute(context={})
    assert datetime.fromisoformat(finished).tzinfo is not None
    assert system.calls[0] == [*UNIT_COMMAND, "start", "bluesky-process.service"]
    assert system.calls[1] == ["systemctl", "show", "--property=InvocationID", "--value", "bluesky-process.service"]
    assert system.calls[2][:2] == ["journalctl", "_SYSTEMD_INVOCATION_ID=4f2c9a"]
    assert "| Loaded 1,592,658 events" in task_log.lines


def test_a_failed_unit_fails_the_task_with_its_log_copied(task_log):
    system = FakeSystem(fail=True)
    op = SystemdUnitOperator(task_id="process", unit="bluesky-process.service", runner=system)
    with pytest.raises(AirflowException, match="bluesky-process.service failed: Job for"):
        op.execute(context={})
    assert "| Committed" in task_log.lines


def test_a_killed_task_stops_its_unit(task_log):
    system = FakeSystem()
    SystemdUnitOperator(task_id="compact", unit="bluesky-compact.service", runner=system).on_kill()
    assert system.calls == [[*UNIT_COMMAND, "stop", "bluesky-compact.service"]]
