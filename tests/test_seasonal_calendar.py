import numpy as np
import pandas as pd
import pytest

from analytics.calendar.seasonal_calendar import seasonal_calendar


def test_calendar_forecasts_next_year_and_lead_in_actions():
    dates = pd.date_range("2020-01-01", periods=60, freq="MS")
    counts = pd.DataFrame(
        {
            "date": dates,
            "division": "Dhaka",
            "disease": "dengue",
            "cases": [
                100 + 2 * index + (500 if date.month == 8 else 0)
                for index, date in enumerate(dates)
            ],
        }
    )

    calendar = seasonal_calendar(counts)

    assert len(calendar) == 12
    assert set(calendar["calendar_year"]) == {2025}
    assert calendar["month"].tolist() == list(range(1, 13))
    peak_month = int(calendar.loc[calendar["expected_cases"].idxmax(), "month"])
    assert peak_month == 8
    assert calendar.loc[calendar["month"] == 5, "action"].iat[0] == "Start prevention campaign"
    assert calendar.loc[calendar["month"] == 6, "action"].iat[0] == "Pre-position supplies"


def test_calendar_requires_two_full_seasons_and_required_columns():
    short = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=18, freq="MS"),
            "division": "Dhaka",
            "disease": "dengue",
            "cases": np.arange(18),
        }
    )
    assert seasonal_calendar(short).empty

    with pytest.raises(ValueError, match="division"):
        seasonal_calendar(short.drop(columns="division"))
