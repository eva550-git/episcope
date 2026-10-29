import pytest
from pyspark.sql import SparkSession

from pipeline.quality.spark_quality import _keep_latest_encounter_versions


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("episcope-spark-quality-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_latest_bronze_version_replaces_old_copies_but_keeps_same_load_duplicates(spark):
    data = spark.sql(
        """
        SELECT * FROM VALUES
            ('E1', 'batch-old', TIMESTAMP '2026-01-01 00:00:00', 'old'),
            ('E1', 'batch-new', TIMESTAMP '2026-01-02 00:00:00', 'new'),
            ('E1', 'batch-new', TIMESTAMP '2026-01-02 00:00:00', 'duplicate'),
            ('E2', 'batch-old', TIMESTAMP '2026-01-01 00:00:00', 'only-version'),
            (CAST(NULL AS STRING), 'batch-old', TIMESTAMP '2026-01-01 00:00:00', 'missing-id-old'),
            (CAST(NULL AS STRING), 'batch-new', TIMESTAMP '2026-01-02 00:00:00', 'missing-id-new')
        AS versions(encounter_id, _batch_id, _load_time, marker)
        """
    )

    result = _keep_latest_encounter_versions(data)
    retained = [(row.encounter_id, row.marker) for row in result.collect()]

    assert set(retained) == {
        ("E1", "new"),
        ("E1", "duplicate"),
        ("E2", "only-version"),
        (None, "missing-id-old"),
        (None, "missing-id-new"),
    }


def test_raw_input_without_bronze_metadata_is_unchanged(spark):
    data = spark.sql(
        "SELECT 'E1' AS encounter_id UNION ALL SELECT 'E1' AS encounter_id"
    )

    assert _keep_latest_encounter_versions(data).count() == 2
