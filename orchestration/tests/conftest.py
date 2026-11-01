import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="the orchestration tests need Airflow installed")

DAGS = Path(__file__).parents[1] / "dags"
sys.path.insert(0, str(DAGS))
