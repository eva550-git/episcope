"""Run distributed silver quality and privacy processing."""

from pipeline.quality.spark_quality import apply_spark_quality_and_privacy_checks


if __name__ == "__main__":
    apply_spark_quality_and_privacy_checks()
