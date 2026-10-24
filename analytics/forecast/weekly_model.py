"""Spark-distributed weekly district/disease forecasts with lagged weather inputs.

Input DataFrame grain is one row per district, disease, and Monday ``week_start``.
The numeric inputs are observed weekly case counts, rainfall, and temperature.
Future weather is unknown: forecasts carry forward the latest observed weather
values. Prediction intervals use empirical training residual quantiles and are
not an assertion of validated forecast accuracy.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

HORIZONS = (1, 2, 3, 4)
MIN_TRAIN_WEEKS = 12
_LAGS = 4
_RIDGE = 1.0


def _validated_history(history: Any, *, minimum_weeks: int) -> Any:
    """Check the public input contract using bounded Spark actions."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, NumericType, TimestampType
    from pyspark.sql.window import Window

    required = {
        "district_id",
        "disease",
        "week_start",
        "cases",
        "rainfall",
        "temperature",
    }
    missing = sorted(required.difference(history.columns))
    if missing:
        raise ValueError(f"weekly history is missing required columns: {missing}")

    schema = {field.name: field.dataType for field in history.schema.fields}
    if not isinstance(schema["week_start"], (DateType, TimestampType)):
        raise TypeError("week_start must have Spark DateType or TimestampType")
    for column in ("cases", "rainfall", "temperature"):
        if not isinstance(schema[column], NumericType):
            raise TypeError(f"{column} must have a numeric Spark type")

    data = history.select(
        F.col("district_id").cast("string").alias("district_id"),
        F.col("disease").cast("string").alias("disease"),
        F.to_date("week_start").alias("week_start"),
        F.col("cases").cast("double").alias("cases"),
        F.col("rainfall").cast("double").alias("rainfall"),
        F.col("temperature").cast("double").alias("temperature"),
    )

    invalid = (
        F.col("district_id").isNull()
        | (F.length(F.trim("district_id")) == 0)
        | F.col("disease").isNull()
        | (F.length(F.trim("disease")) == 0)
        | F.col("week_start").isNull()
        | (F.dayofweek("week_start") != 2)
    )
    for column in ("cases", "rainfall", "temperature"):
        value = F.col(column)
        invalid = invalid | value.isNull() | F.isnan(value) | (
            F.abs(value) == F.lit(float("inf"))
        )
    invalid = invalid | (F.col("cases") < 0) | (F.col("rainfall") < 0)
    if data.filter(invalid).limit(1).count():
        raise ValueError(
            "weekly history requires non-empty keys, finite non-null values, "
            "nonnegative cases/rainfall, and Monday week_start dates"
        )

    keys = ["district_id", "disease", "week_start"]
    if data.groupBy(*keys).count().filter(F.col("count") != 1).limit(1).count():
        raise ValueError("weekly history must be unique by district, disease, week_start")

    series_window = Window.partitionBy("district_id", "disease").orderBy("week_start")
    discontinuous = data.withColumn(
        "_previous_week", F.lag("week_start").over(series_window)
    ).filter(
        F.col("_previous_week").isNotNull()
        & (F.datediff("week_start", "_previous_week") != 7)
    )
    if discontinuous.limit(1).count():
        raise ValueError("each district/disease history must contain consecutive weeks")
    if (
        data.groupBy("district_id", "disease")
        .count()
        .filter(F.col("count") < minimum_weeks)
        .limit(1)
        .count()
    ):
        raise ValueError(
            f"each district/disease series must contain at least {minimum_weeks} weeks"
        )
    return data


def _lagged_features(cases: np.ndarray, rainfall: np.ndarray, temperature: np.ndarray) -> np.ndarray:
    """Build intercept plus four lags for each observed variable."""
    return np.asarray(
        [1.0]
        + [float(cases[-lag]) for lag in range(1, _LAGS + 1)]
        + [float(rainfall[-lag]) for lag in range(1, _LAGS + 1)]
        + [float(temperature[-lag]) for lag in range(1, _LAGS + 1)],
        dtype=float,
    )


def _fit_and_forecast(
    series: pd.DataFrame,
    *,
    origin_index: int,
    min_train_weeks: int,
) -> list[dict[str, Any]]:
    """Fit/forecast one series prefix; caller keeps data grouped in Spark."""
    prefix = series.iloc[: origin_index + 1]
    if len(prefix) < min_train_weeks:
        return []
    cases = prefix["cases"].to_numpy(dtype=float)
    rainfall = prefix["rainfall"].to_numpy(dtype=float)
    temperature = prefix["temperature"].to_numpy(dtype=float)
    if len(prefix) <= _LAGS:
        return []

    features = np.vstack(
        [
            _lagged_features(cases[:index], rainfall[:index], temperature[:index])
            for index in range(_LAGS, len(prefix))
        ]
    )
    targets = cases[_LAGS:]
    penalty = np.eye(features.shape[1], dtype=float) * _RIDGE
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(features.T @ features + penalty, features.T @ targets)
    residuals = targets - features @ coefficients
    residual_low, residual_high = np.quantile(residuals, [0.1, 0.9])

    origin = pd.Timestamp(prefix.iloc[-1]["week_start"])
    district_id = str(prefix.iloc[-1]["district_id"])
    disease = str(prefix.iloc[-1]["disease"])
    simulated_cases = list(cases)
    simulated_rainfall = list(rainfall)
    simulated_temperature = list(temperature)
    output: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        row = _lagged_features(
            np.asarray(simulated_cases),
            np.asarray(simulated_rainfall),
            np.asarray(simulated_temperature),
        )
        prediction = max(0.0, float(row @ coefficients))
        interval_scale = float(np.sqrt(horizon))
        lower = max(0.0, prediction + float(residual_low) * interval_scale)
        upper = max(prediction, prediction + float(residual_high) * interval_scale)
        lower = min(lower, prediction)
        forecast_week = origin + pd.Timedelta(weeks=horizon)
        output.append(
            {
                "district_id": district_id,
                "disease": disease,
                "origin_week": origin.date(),
                "forecast_week": forecast_week.date(),
                "horizon": horizon,
                "forecast_cases": prediction,
                "lower_80": lower,
                "upper_80": upper,
            }
        )
        simulated_cases.append(prediction)
        # No future weather feed is assumed; hold the last known observations.
        simulated_rainfall.append(rainfall[-1])
        simulated_temperature.append(temperature[-1])
    return output


def forecast_weekly(history: Any, *, min_train_weeks: int = MIN_TRAIN_WEEKS) -> Any:
    """Return four weekly forecasts per district/disease using Spark group UDFs.

    Parameters
    ----------
    history:
        Spark DataFrame with ``district_id`` and ``disease`` string-compatible
        identifiers; ``week_start`` as DateType/TimestampType; and numeric
        ``cases``, ``rainfall``, ``temperature``. Rows must be unique, contiguous
        weekly Mondays with finite values; cases and rainfall must be >= 0.
    min_train_weeks:
        Minimum number of observed weeks required for each series.

    The returned Spark DataFrame has one row for each series and horizon 1–4.
    """
    if min_train_weeks < _LAGS + 1:
        raise ValueError(f"min_train_weeks must be at least {_LAGS + 1}")
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    data = _validated_history(history, minimum_weeks=min_train_weeks)
    output_schema = StructType(
        [
            StructField("district_id", StringType(), False),
            StructField("disease", StringType(), False),
            StructField("origin_week", DateType(), False),
            StructField("forecast_week", DateType(), False),
            StructField("horizon", IntegerType(), False),
            StructField("forecast_cases", DoubleType(), False),
            StructField("lower_80", DoubleType(), False),
            StructField("upper_80", DoubleType(), False),
        ]
    )

    def apply_series(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("week_start").reset_index(drop=True)
        rows = _fit_and_forecast(
            pdf, origin_index=len(pdf) - 1, min_train_weeks=min_train_weeks
        )
        return pd.DataFrame(rows, columns=[field.name for field in output_schema.fields])

    return data.groupBy("district_id", "disease").applyInPandas(
        apply_series, schema=output_schema
    )
