from datetime import date, timedelta

import pandas as pd
import pytest

from analytics.forecast.evaluation import _evaluate_series, _summarize_evaluations
from analytics.forecast.weekly_model import (
    MIN_TRAIN_WEEKS,
    _fit_and_forecast,
    _lagged_features,
)


def weekly_fixture(weeks=28):
    """Generated, deterministic weekly district/disease/weather history."""
    start = date(2024, 1, 1)  # Monday
    return pd.DataFrame(
        [
            {
                "district_id": "D-001",
                "disease": "dengue",
                "week_start": start + timedelta(weeks=index),
                "cases": 10.0,
                "rainfall": 12.0,
                "temperature": 27.0,
            }
            for index in range(weeks)
        ]
    )


def test_forecast_emits_all_horizons_and_ordered_80_percent_interval():
    history = weekly_fixture()
    predictions = _fit_and_forecast(
        history, origin_index=len(history) - 1, min_train_weeks=MIN_TRAIN_WEEKS
    )

    assert [prediction["horizon"] for prediction in predictions] == [1, 2, 3, 4]
    assert all(prediction["forecast_cases"] == pytest.approx(10.0) for prediction in predictions)
    assert all(
        prediction["lower_80"]
        <= prediction["forecast_cases"]
        <= prediction["upper_80"]
        for prediction in predictions
    )
    assert all(prediction["origin_week"] == date(2024, 7, 8) for prediction in predictions)


def test_model_feature_vector_contains_lagged_rainfall_and_temperature():
    features = _lagged_features(
        pd.Series([1.0, 2.0, 3.0, 4.0]).to_numpy(),
        pd.Series([10.0, 20.0, 30.0, 40.0]).to_numpy(),
        pd.Series([5.0, 6.0, 7.0, 8.0]).to_numpy(),
    )

    assert features.tolist() == [
        1.0,
        4.0,
        3.0,
        2.0,
        1.0,
        40.0,
        30.0,
        20.0,
        10.0,
        8.0,
        7.0,
        6.0,
        5.0,
    ]


def test_rolling_origin_metrics_match_constant_fixture():
    scored = _evaluate_series(weekly_fixture(), min_train_weeks=MIN_TRAIN_WEEKS)
    metrics = _summarize_evaluations(scored)

    assert metrics["horizon"].tolist() == [1, 2, 3, 4]
    assert metrics["mae"].tolist() == pytest.approx([0.0] * 4, abs=1e-10)
    assert metrics["coverage_80"].tolist() == pytest.approx([1.0] * 4)
    assert metrics["seasonal_naive_mae"].tolist() == pytest.approx([0.0] * 4)


def test_rolling_origin_predictions_only_use_data_at_each_origin():
    history = weekly_fixture()
    baseline_scores = _evaluate_series(history, min_train_weeks=MIN_TRAIN_WEEKS)
    changed_future = history.copy()
    changed_future.loc[changed_future.index >= MIN_TRAIN_WEEKS, "cases"] = 400.0
    future_scores = _evaluate_series(changed_future, min_train_weeks=MIN_TRAIN_WEEKS)

    initial_origin = date(2024, 3, 18)
    before_future_changes = baseline_scores.loc[
        baseline_scores["origin_week"] == initial_origin, "forecast_cases"
    ].tolist()
    after_future_changes = future_scores.loc[
        future_scores["origin_week"] == initial_origin, "forecast_cases"
    ].tolist()
    assert after_future_changes == pytest.approx(before_future_changes)
