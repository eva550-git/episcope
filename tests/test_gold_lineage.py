import pytest

from pipeline.analytics_runner import _read_counts
from pipeline.common import get_spark
from pipeline.gold.create_gold_marts import _build_spark_gold


@pytest.fixture(scope="module")
def spark():
    session = get_spark("episcope-gold-lineage-tests")
    yield session
    session.stop()


def test_gold_uses_latest_daily_count_revision_and_preserves_bronze_batch(tmp_path, spark):
    bronze_path = tmp_path / "bronze_counts"
    spark.sql(
        """
        SELECT * FROM VALUES
            ('2025-01-06', 'BD1004', 'dengue', '4',
             'batch-old', TIMESTAMP '2025-01-07 00:00:00'),
            ('2025-01-06', 'BD1004', 'dengue', '9',
             'batch-late', TIMESTAMP '2025-01-09 00:00:00')
        AS counts(date, district_id, disease, cases, _batch_id, _load_time)
        """
    ).write.mode("overwrite").partitionBy("_batch_id").parquet(str(bronze_path))

    reference_path = tmp_path / "district_ref.csv"
    reference_path.write_text(
        "district_id,division_id,division,district,population\n"
        "BD1004,BD10,Barisal,Barguna,100000\n",
        encoding="utf-8",
    )

    result = _build_spark_gold(
        spark,
        tmp_path / "unused.csv",
        reference_path,
        bronze_path,
    ).select("cases", "load_batch_id").collect()

    assert len(result) == 1
    assert result[0]["cases"] == 9
    assert result[0]["load_batch_id"] == "batch-late"


def test_analytics_reads_the_latest_bronze_daily_count_revision(tmp_path, spark):
    bronze_path = tmp_path / "bronze_counts"
    spark.sql(
        """
        SELECT * FROM VALUES
            ('2025-01-06', 'BD1004', 'dengue', '4',
             'batch-old', TIMESTAMP '2025-01-07 00:00:00'),
            ('2025-01-06', 'BD1004', 'dengue', '9',
             'batch-late', TIMESTAMP '2025-01-09 00:00:00')
        AS counts(date, district_id, disease, cases, _batch_id, _load_time)
        """
    ).write.mode("overwrite").partitionBy("_batch_id").parquet(str(bronze_path))

    counts = _read_counts(spark, tmp_path / "unused.csv", bronze_path).collect()

    assert len(counts) == 1
    assert counts[0]["cases"] == 9
