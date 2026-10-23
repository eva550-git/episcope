"""Shared helpers for the EpiScope pipeline: lake paths and the Spark session."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
LAKE = Path(os.environ.get("EPISCOPE_LAKE", ROOT / "lake"))   # layered lake (git-ignored)


def uri(path):
    """file:// URI for a local path (works with Windows drive letters)."""
    return Path(path).resolve().as_uri()


def get_spark(app_name="episcope"):
    """Local-mode Spark sized for a 8 GB laptop. Override with SPARK_DRIVER_MEMORY / SPARK_MASTER."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark