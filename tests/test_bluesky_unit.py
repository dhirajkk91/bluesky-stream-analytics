import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "deploy" / "airflow" / "bluesky-unit"


def run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["sh", str(SCRIPT), *args], capture_output=True, text=True)


@pytest.mark.parametrize(
    "args",
    [
        ("start", "bluesky-collect.service"),
        ("start", "bluesky-upload.service"),
        ("stop", "bluesky-check.service"),
        ("start", "sshd.service"),
        ("start", "bluesky-process.service sshd.service"),
        ("start", "bluesky-marts@../../etc.service"),
        ("start", "bluesky-marts@2026-10-31.service\nsshd.service"),
        ("restart", "bluesky-process.service"),
        ("start",),
        ("start", "bluesky-process.service", "extra"),
    ],
)
def test_only_the_projects_job_units_are_allowed(args):
    result = run(*args)
    assert result.returncode == 2
    assert result.stderr.startswith(("usage:", "bluesky-unit:"))


@pytest.mark.parametrize(
    "unit",
    [
        "bluesky-process.service",
        "bluesky-project.service",
        "bluesky-handles.service",
        "bluesky-compact.service",
        "bluesky-vacuum.service",
        "bluesky-export.service",
        "bluesky-marts@2026-10-31.service",
    ],
)
def test_the_job_units_get_through_to_systemctl(unit):
    # Past the check, the script hands over to systemctl, which isn't usable here;
    # what matters is that the check let it through.
    result = run("start", unit)
    assert "not one of the project's job units" not in result.stderr
    assert result.returncode != 2 or "usage" not in result.stderr
