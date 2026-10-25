import os
import sys
from pathlib import Path

import pytest

from bluesky_stream_analytics.spark import RUNTIME_JAR

JARS_DIR = Path(os.environ.get("ICEBERG_JARS_DIR", Path(__file__).parent.parent / ".jars"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """
    One local Spark session for the whole run.

    It has an Iceberg Hadoop catalog when the runtime jar is in .jars/ (or
    ICEBERG_JARS_DIR); otherwise it's plain Spark and Iceberg tests are skipped.
    """
    pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if (JARS_DIR / RUNTIME_JAR).exists():
        from bluesky_stream_analytics.spark import build_session

        session = build_session(str(tmp_path_factory.mktemp("warehouse")), JARS_DIR, "hadoop", "1g")
        session.has_iceberg = True
    else:
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        session.has_iceberg = False
    yield session
    session.stop()
