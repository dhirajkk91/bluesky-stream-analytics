"""
An Airflow operator that runs a job by starting its systemd unit.

The jobs stay systemd services: they run as the unprivileged `bluesky` user,
with their AWS keys read by systemd from root-only files, inside their sandbox,
behind the lake lock, and they report to healthchecks.io as before. Airflow
decides when each one starts and what it waits for.

Starting a unit goes through `bluesky-unit`, a root-owned script that the
`airflow` user may run with sudo and that accepts only the project's job units.
The unit's log for this run is copied into the task's log, so the Airflow UI
shows what the job printed.
"""

import subprocess
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException

UNIT_COMMAND = ("sudo", "--non-interactive", "/usr/local/sbin/bluesky-unit")
LOG_LINES = 300

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess]


def run(args: Sequence[str]) -> subprocess.CompletedProcess:
    return subprocess.run(list(args), capture_output=True, text=True)


class SystemdUnitOperator(BaseOperator):
    """
    Start a oneshot systemd unit, wait for it to finish, and fail if it failed.
    Returns when it finished, as an ISO timestamp, for tasks that wait on it.
    """

    template_fields: Sequence[str] = ("unit",)
    ui_color = "#dfe9f5"

    def __init__(self, *, unit: str, runner: Runner = run, **kwargs) -> None:
        super().__init__(**kwargs)
        self.unit = unit
        self.runner = runner

    def execute(self, context) -> str:
        started = datetime.now(UTC)
        self.log.info("Starting %s", self.unit)
        # For a oneshot unit, `systemctl start` returns once the job has finished.
        result = self.runner([*UNIT_COMMAND, "start", self.unit])
        self._copy_journal()
        if result.returncode != 0:
            reason = result.stderr.strip() or f"exit status {result.returncode}"
            raise AirflowException(f"{self.unit} failed: {reason}")
        finished = datetime.now(UTC)
        self.log.info("%s finished in %.0f seconds", self.unit, (finished - started).total_seconds())
        return finished.isoformat()

    def on_kill(self) -> None:
        self.log.info("Stopping %s", self.unit)
        self.runner([*UNIT_COMMAND, "stop", self.unit])

    def _copy_journal(self) -> None:
        """Copy what the unit logged during this run into the task log."""
        shown = self.runner(["systemctl", "show", "--property=InvocationID", "--value", self.unit])
        invocation = shown.stdout.strip()
        if not invocation:
            self.log.warning("No log found for %s", self.unit)
            return
        journal = self.runner(
            ["journalctl", f"_SYSTEMD_INVOCATION_ID={invocation}", "--output=cat", "--no-pager", f"--lines={LOG_LINES}"]
        )
        for line in journal.stdout.splitlines():
            self.log.info("| %s", line)
