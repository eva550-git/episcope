from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from analytics.detection.ears_detector import detect_ears
from analytics.detection.ears_evaluation import (
    evaluate_ears,
    evaluate_reporting_artefacts,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("episcope-ears-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _daily_fixture(spark):
    first_day = date(2024, 1, 1)
    values = []
    for district, spike_day in (("A", 7), ("B", 8)):
        for offset in range(10):
            values.append(
                f"('{district}', 'dengue', DATE '{first_day + timedelta(days=offset)}', "
                f"{9 if offset == spike_day else 0})"
            )
    return spark.sql(
        "SELECT * FROM VALUES "
        + ", ".join(values)
        + " AS counts(district_id, disease, date, cases)"
    )


def test_spark_window_c1_and_evaluation_metrics(spark):
    counts = _daily_fixture(spark)
    scored = detect_ears(
        counts,
        baseline_days=7,
        alert_threshold=3.0,
        min_baseline_days=7,
    )
    actual = {
        (row.district_id, row.date): row
        for row in scored.select(
            "district_id", "date", "baseline_mean", "c1_score", "alert"
        ).collect()
    }
    spike_date_a = date(2024, 1, 8)
    assert actual[("A", spike_date_a)].baseline_mean == 0.0
    assert actual[("A", spike_date_a)].c1_score == 9.0
    assert actual[("A", spike_date_a)].alert is True
    assert actual[("A", date(2024, 1, 7))].alert is False

    truth = spark.sql(
        "SELECT 'A' AS district_id, 'dengue' AS disease, "
        "DATE '2024-01-08' AS start_date, DATE '2024-01-09' AS end_date"
    )
    report = evaluate_ears(scored, truth, thresholds=(5.0, 10.0))

    assert report["sensitivity"] == 1.0
    assert report["false_alerts_per_district_year"] == 0.5
    assert report["median_detection_delay_days"] == 0.0
    assert report["threshold_tradeoff"] == [
        {
            "threshold": 5.0,
            "sensitivity": 1.0,
            "false_alerts_per_district_year": 0.5,
            "median_detection_delay_days": 0.0,
        },
        {
            "threshold": 10.0,
            "sensitivity": 0.0,
            "false_alerts_per_district_year": 0.0,
            "median_detection_delay_days": None,
        },
    ]

    artefacts = spark.sql(
        "SELECT 'B' AS district_id, DATE '2024-01-09' AS start_date, "
        "DATE '2024-01-09' AS end_date"
    )
    artefact_report = evaluate_reporting_artefacts(scored, artefacts)
    assert artefact_report == {
        "total_alert_days": 2,
        "artefact_alert_days": 1,
        "artefact_alert_fraction": 0.5,
    }


def test_evaluator_accepts_explicit_csv_path(spark, tmp_path):
    scored = detect_ears(
        _daily_fixture(spark),
        baseline_days=7,
        alert_threshold=3.0,
        min_baseline_days=7,
    )
    truth_path = tmp_path / "explicit_truth.csv"
    truth_path.write_text(
        "district_id,disease,start_date,end_date\n"
        "A,dengue,2024-01-08,2024-01-09\n",
        encoding="utf-8",
    )

    report = evaluate_ears(scored, truth_path, thresholds=())
    assert report["ground_truth_outbreaks"] == 1
    assert report["sensitivity"] == 1.0
    assert report["threshold_tradeoff"] == []


def test_detector_validates_baseline_parameters(spark):
    with pytest.raises(ValueError, match="cannot exceed"):
        detect_ears(_daily_fixture(spark), baseline_days=5, min_baseline_days=6)
