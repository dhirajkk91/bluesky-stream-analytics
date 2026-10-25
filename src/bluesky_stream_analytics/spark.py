"""
Spark session with an Iceberg catalog named `lake`.

On the server the catalog is AWS Glue with table files in S3. Locally and in
tests it's a Hadoop catalog in a plain folder. The Iceberg jars are read from
a local folder (see deploy/fetch-jars.sh), so starting a job never downloads
anything.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.12.0"
RUNTIME_JAR = f"iceberg-spark-runtime-4.1_2.13-{ICEBERG_VERSION}.jar"
AWS_BUNDLE_JAR = f"iceberg-aws-bundle-{ICEBERG_VERSION}.jar"
CATALOG = "lake"


def build_session(
    warehouse: str,
    jars_dir: Path,
    catalog_type: str = "glue",
    driver_memory: str = "4g",
    region: str = "us-east-1",
) -> SparkSession:
    """
    Start a local Spark session using every core, with the `lake` Iceberg catalog.

    catalog_type is "glue" (warehouse is an s3:// path) or "hadoop" (warehouse
    is a local folder).
    """
    jars = [jars_dir / RUNTIME_JAR]
    if catalog_type == "glue":
        jars.append(jars_dir / AWS_BUNDLE_JAR)
    missing = [str(j) for j in jars if not j.exists()]
    if missing:
        raise FileNotFoundError(f"Iceberg jars not found: {', '.join(missing)}")

    # Python workers must run the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    catalog = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("bluesky-stream-analytics")
        .config("spark.jars", ",".join(str(j) for j in jars))
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", catalog_type)
        .config(f"{catalog}.warehouse", warehouse)
    )
    if catalog_type == "glue":
        builder = builder.config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO").config(
            f"{catalog}.client.region", region
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
