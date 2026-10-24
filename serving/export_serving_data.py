"""Build dashboard-ready surveillance, forecast, alert, and resource summaries."""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.calendar.seasonal_calendar import seasonal_calendar
from analytics.optimise.resource_allocator import optimize_resource_portfolio

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "serving"
REQUIRED_GOLD_COLUMNS = {"week_start", "division", "district", "disease", "cases"}
FORECAST_HORIZON_WEEKS = 4
RECENT_WEEKS = 4
BASELINE_WEEKS = 8
ADMISSION_RATE = 0.10
AVERAGE_STAY_DAYS = 7
TEST_KITS_PER_CASE = 1.0
SALINE_UNITS_PER_ADMISSION = 2.0
BOUNDARY_SIMPLIFICATION_DEGREES = 0.001
SMALL_CELL_THRESHOLD = 5
RESOURCE_COSTS = {
    "beds": 15_000.0,
    "iv_saline": 120.0,
    "ns1_kits": 250.0,
    "ors": 25.0,
    "antimalarials": 80.0,
}
RESOURCE_LABELS = {
    "beds": "Available beds",
    "iv_saline": "IV saline units",
    "ns1_kits": "NS1 test kits",
    "ors": "ORS units",
    "antimalarials": "Antimalarial courses",
}


def _records(frame: pd.DataFrame) -> list[dict]:
    """Convert a frame to JSON-safe records, including missing numeric values."""
    clean = frame.astype(object).where(pd.notna(frame), None)
    return json.loads(clean.to_json(orient="records", date_format="iso"))


def _public_count(value: float | int) -> int | None:
    numeric = float(value)
    if numeric <= 0:
        return 0
    if numeric < SMALL_CELL_THRESHOLD:
        return None
    return int(round(numeric))


def _generate_stock_snapshot(
    facilities_path: Path,
    stock_path: Path,
    snapshot_date: str,
) -> pd.DataFrame:
    """Create an explicitly synthetic stock snapshot when no S6 feed is supplied."""
    facilities = pd.read_csv(facilities_path)
    required = {"facility_id", "district_id", "beds"}
    if not required.issubset(facilities.columns):
        raise ValueError(f"Facility registry must contain {sorted(required)}.")

    facilities["beds"] = pd.to_numeric(facilities["beds"], errors="raise").clip(lower=0)
    rng = np.random.default_rng(2026)
    beds = facilities["beds"].astype(int).to_numpy()
    snapshot = facilities[["facility_id", "district_id"]].copy()
    snapshot["snapshot_date"] = snapshot_date
    snapshot["beds_occupied"] = np.minimum(
        beds,
        np.rint(beds * rng.uniform(0.45, 0.85, len(facilities))).astype(int),
    )
    snapshot["iv_saline"] = np.rint(beds * rng.uniform(0.8, 1.8, len(facilities))).astype(int)
    snapshot["ns1_kits"] = np.rint(beds * rng.uniform(0.4, 1.2, len(facilities))).astype(int)
    snapshot["ors"] = np.rint(beds * rng.uniform(1.0, 2.5, len(facilities))).astype(int)
    snapshot["antimalarials"] = np.rint(beds * rng.uniform(0.15, 0.6, len(facilities))).astype(int)
    snapshot["stock_source"] = "synthetic_planning_scenario"
    stock_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(stock_path, index=False)
    return snapshot


def _read_stock_snapshot(
    facilities_path: Path,
    stock_path: Path,
    snapshot_date: str,
) -> tuple[pd.DataFrame, bool]:
    if not stock_path.exists():
        return _generate_stock_snapshot(facilities_path, stock_path, snapshot_date), True

    stock = pd.read_csv(stock_path)
    stock_resources = [resource for resource in RESOURCE_COSTS if resource != "beds"]
    required = {"facility_id", "district_id", "beds_occupied", "snapshot_date", *stock_resources}
    if not required.issubset(stock.columns):
        raise ValueError(f"Stock snapshot must contain {sorted(required)}.")
    if stock["facility_id"].duplicated().any():
        raise ValueError("Stock snapshot must have one row per facility.")
    numeric_columns = stock_resources + ["beds_occupied"]
    stock[numeric_columns] = stock[numeric_columns].apply(pd.to_numeric, errors="raise")
    if (stock[numeric_columns] < 0).any().any():
        raise ValueError("Stock and occupied-bed values cannot be negative.")
    is_synthetic = (
        "stock_source" in stock
        and stock["stock_source"].eq("synthetic_planning_scenario").all()
    )
    return stock, bool(is_synthetic)


def _simplify_ring(points: list[list[float]]) -> list[list[float]]:
    """Reduce boundary vertices below screen-level detail while retaining ring shape."""
    coordinates = np.asarray(points, dtype=float)[:, :2]
    if len(coordinates) <= 4:
        return points

    keep = np.zeros(len(coordinates), dtype=bool)
    keep[[0, len(coordinates) - 1]] = True
    stack = [(0, len(coordinates) - 1)]
    tolerance_squared = BOUNDARY_SIMPLIFICATION_DEGREES**2
    while stack:
        start_index, end_index = stack.pop()
        if end_index <= start_index + 1:
            continue
        segment = coordinates[end_index] - coordinates[start_index]
        candidates = coordinates[start_index + 1:end_index]
        segment_length_squared = float(segment @ segment)
        if segment_length_squared:
            projection = np.clip(
                ((candidates - coordinates[start_index]) @ segment) / segment_length_squared,
                0.0,
                1.0,
            )
            difference = candidates - (coordinates[start_index] + projection[:, None] * segment)
        else:
            difference = candidates - coordinates[start_index]
        distances_squared = np.einsum("ij,ij->i", difference, difference)
        furthest_offset = int(np.argmax(distances_squared))
        if distances_squared[furthest_offset] > tolerance_squared:
            furthest_index = start_index + 1 + furthest_offset
            keep[furthest_index] = True
            stack.extend(((start_index, furthest_index), (furthest_index, end_index)))

    indexes = np.flatnonzero(keep)
    return [points[index] for index in indexes] if len(indexes) >= 4 else points


def _signed_ring_area(points: list[list[float]]) -> float:
    coordinates = np.asarray(points, dtype=float)[:, :2]
    return float(
        np.sum(
            coordinates[:-1, 0] * coordinates[1:, 1]
            - coordinates[1:, 0] * coordinates[:-1, 1]
        )
        / 2
    )


def _simplify_polygon(rings: list[list[list[float]]]) -> list[list[list[float]]]:
    simplified_rings = []
    for index, ring in enumerate(rings):
        simplified = _simplify_ring(ring)
        area = _signed_ring_area(simplified)
        should_be_clockwise = index == 0
        if (should_be_clockwise and area > 0) or (not should_be_clockwise and area < 0):
            simplified = list(reversed(simplified))
        simplified_rings.append(simplified)
    return simplified_rings


def _simplify_geometry(geometry: dict) -> dict:
    geometry_type = geometry["type"]
    if geometry_type == "Polygon":
        coordinates = _simplify_polygon(geometry["coordinates"])
    elif geometry_type == "MultiPolygon":
        coordinates = [_simplify_polygon(polygon) for polygon in geometry["coordinates"]]
    else:
        raise ValueError(f"Unsupported district boundary geometry: {geometry_type}")
    return {"type": geometry_type, "coordinates": coordinates}


def _write_simplified_boundaries(source: Path, destination: Path) -> None:
    boundaries = json.loads(source.read_text(encoding="utf-8"))
    features = []
    for feature in boundaries.get("features", []):
        properties = feature.get("properties", {})
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "adm2_pcode": properties["adm2_pcode"],
                    "adm2_en": properties["adm2_en"],
                    "adm1_en": properties.get("adm1_en", ""),
                },
                "geometry": _simplify_geometry(feature["geometry"]),
            }
        )
    simplified = {"type": "FeatureCollection", "features": features}
    destination.write_text(
        json.dumps(simplified, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )


def _weekly_forecast(
    weekly: pd.DataFrame, group_columns: list[str], group_values: tuple
) -> list[dict]:
    group = weekly
    for column, value in zip(group_columns, group_values):
        group = group[group[column] == value]
    series = group.groupby("week_start", as_index=False)["cases"].sum().sort_values("week_start")
    if series.empty:
        return []

    dates = pd.date_range(series["week_start"].min(), series["week_start"].max(), freq="7D")
    series = series.set_index("week_start").reindex(dates, fill_value=0).rename_axis("week_start").reset_index()
    recent = series["cases"].tail(RECENT_WEEKS)
    baseline = series["cases"].tail(RECENT_WEEKS + BASELINE_WEEKS).head(BASELINE_WEEKS)
    expected = max(0.0, float(recent.mean()))
    spread = float(baseline.std(ddof=1)) if len(baseline) > 1 else 0.0
    margin = 1.2816 * spread

    return [
        {
            **dict(zip(group_columns, group_values)),
            "week_start": (series["week_start"].max() + pd.Timedelta(weeks=offset)).date().isoformat(),
            "forecast_cases": round(expected, 2),
            "lower_80": round(max(0.0, expected - margin), 2),
            "upper_80": round(expected + margin, 2),
        }
        for offset in range(1, FORECAST_HORIZON_WEEKS + 1)
    ]


def _build_alerts(weekly: pd.DataFrame) -> list[dict]:
    rows = []
    latest_date = weekly["week_start"].max()
    for (district_id, district, division, disease), group in weekly.groupby(
        ["district_id", "district", "division", "disease"], sort=True
    ):
        values = group.groupby("week_start")["cases"].sum().sort_index()
        last_12 = values.loc[values.index <= latest_date].tail(RECENT_WEEKS + BASELINE_WEEKS)
        if len(last_12) < RECENT_WEEKS + BASELINE_WEEKS:
            continue

        baseline = last_12.iloc[:-RECENT_WEEKS]
        recent = last_12.iloc[-RECENT_WEEKS:]
        if float(baseline.sum()) < SMALL_CELL_THRESHOLD:
            continue
        baseline_mean = float(baseline.mean())
        baseline_std = float(baseline.std(ddof=0))
        recent_mean = float(recent.mean())
        z_score = (recent_mean - baseline_mean) / max(baseline_std, 1.0)
        growth_pct = ((recent_mean - baseline_mean) / baseline_mean * 100.0) if baseline_mean else (
            100.0 if recent_mean > 0 else 0.0
        )
        recent_cases = float(recent.sum())
        if recent_cases >= SMALL_CELL_THRESHOLD:
            rows.append(
                {
                    "district_id": district_id,
                    "district": district,
                    "division": division,
                    "disease": disease,
                    "recent_cases_per_week": _public_count(recent_mean),
                    "baseline_cases_per_week": _public_count(baseline_mean),
                    "growth_pct": round(growth_pct, 1),
                    "z_score": round(z_score, 2),
                    "recent_cases": _public_count(recent_cases),
                    "latest_week": latest_date.date().isoformat(),
                }
            )
    return sorted(
        rows,
        key=lambda row: (-row["z_score"], -(row["recent_cases_per_week"] or 0)),
    )


def _build_resource_demand(
    forecast: pd.DataFrame,
    district_ref: pd.DataFrame,
    facilities: pd.DataFrame,
    stock: pd.DataFrame,
) -> pd.DataFrame:
    forecast_cases = forecast.pivot_table(
        index="district_id",
        columns="disease",
        values="forecast_cases",
        aggfunc="sum",
        fill_value=0,
    )
    reference = district_ref.set_index("district_id")
    bed_stock = facilities.groupby("district_id")["beds"].sum().to_dict()
    stock_resources = [resource for resource in RESOURCE_COSTS if resource != "beds"]
    stocks = stock.groupby("district_id")[stock_resources].sum()
    rows = []
    for district_id in district_ref["district_id"].astype(str):
        disease_cases = forecast_cases.loc[district_id] if district_id in forecast_cases.index else pd.Series(dtype=float)
        all_cases = float(disease_cases.sum())
        admissions = all_cases * ADMISSION_RATE
        needs = {
            "beds": admissions * AVERAGE_STAY_DAYS / 28,
            "iv_saline": admissions * SALINE_UNITS_PER_ADMISSION,
            "ns1_kits": float(disease_cases.get("dengue", 0.0)) * TEST_KITS_PER_CASE,
            "ors": float(disease_cases.get("diarrhoea", 0.0)) * 1.2,
            "antimalarials": float(disease_cases.get("malaria", 0.0)) * 0.9,
        }
        local_stock = stocks.loc[district_id].to_dict() if district_id in stocks.index else {}
        for resource, amount in needs.items():
            available = float(bed_stock.get(district_id, 0)) if resource == "beds" else float(local_stock.get(resource, 0))
            rows.append(
                {
                    "district_id": district_id,
                    "resource": resource,
                    "demand": max(0.0, float(amount)),
                    "current_stock": max(0.0, available),
                    "population": float(reference.loc[district_id, "population"]),
                    "unit_cost": RESOURCE_COSTS[resource],
                    "severity_weight": 1.0 if resource in {"beds", "iv_saline"} else 0.8,
                }
            )
    return pd.DataFrame(rows)


def _population_baseline(demand: pd.DataFrame, budget: float) -> pd.DataFrame:
    rows = []
    for resource, group in demand.groupby("resource", sort=True):
        supply = float(group["current_stock"].sum())
        total_demand = float(group["demand"].sum())
        total_cost = float(group["unit_cost"].iloc[0])
        amount = min(supply, total_demand, budget / total_cost if total_cost else 0.0)
        population = group["population"].clip(lower=0)
        shares = population / population.sum() if population.sum() else population * 0 + 1 / len(group)
        baseline = np.minimum((shares * amount).to_numpy(), group["demand"].to_numpy())
        for (_, row), allocated in zip(group.iterrows(), baseline):
            rows.append(
                {
                    "district_id": row["district_id"],
                    "resource": resource,
                    "baseline_allocation": min(float(row["demand"]), float(allocated)),
                }
            )
    baseline = pd.DataFrame(rows)
    if not baseline.empty:
        costs = demand.set_index(["district_id", "resource"])["unit_cost"]
        baseline["unit_cost"] = [
            costs.loc[(row.district_id, row.resource)]
            for row in baseline.itertuples(index=False)
        ]
        total_cost = float((baseline["baseline_allocation"] * baseline["unit_cost"]).sum())
        if total_cost > budget and total_cost > 0:
            baseline["baseline_allocation"] *= budget / total_cost
    return baseline.drop(columns=["unit_cost"], errors="ignore")


def _solve_allocation_scenarios(
    demand: pd.DataFrame,
    max_budget: float,
) -> tuple[list[dict], list[dict], pd.DataFrame]:
    fractions = [0.20, 0.40, 0.60, 0.80, 1.00]
    equity_levels = [0.0, 0.25, 0.50]
    scenario_grid = []
    curve = []
    default_allocations = pd.DataFrame()
    for fraction in fractions:
        for equity in equity_levels:
            budget = max_budget * fraction
            optimized = optimize_resource_portfolio(demand, budget, equity)
            baseline = _population_baseline(demand, budget)
            result = optimized.merge(baseline, on=["district_id", "resource"], how="left")
            result["baseline_allocation"] = result["baseline_allocation"].fillna(0.0)
            total_demand = float(result["demand"].sum())
            optimized_fill = float(result["allocated"].sum()) / total_demand if total_demand else 1.0
            baseline_fill = float(result["baseline_allocation"].sum()) / total_demand if total_demand else 1.0
            denominator = result["demand"].replace(0, np.nan)
            coverage = (result["allocated"] / denominator).fillna(0.0).groupby(result["district_id"]).mean()
            baseline_coverage = (
                (result["baseline_allocation"] / denominator)
                .fillna(0.0)
                .groupby(result["district_id"])
                .mean()
            )

            def equity_index(values: pd.Series) -> float:
                ordered = np.sort(values.to_numpy(dtype=float))
                if len(ordered) == 0 or ordered.sum() == 0:
                    return 100.0
                ranks = np.arange(1, len(ordered) + 1)
                gini = (2 * np.sum(ranks * ordered) / (len(ordered) * ordered.sum())) - (
                    len(ordered) + 1
                ) / len(ordered)
                return max(0.0, min(100.0, (1 - gini) * 100))

            allocation_rows = _records(
                result.assign(
                    demand=result["demand"].map(_public_count),
                    current_stock=result["current_stock"].map(_public_count),
                    allocated=result["allocated"].map(_public_count),
                    unmet=result["unmet"].map(_public_count),
                    baseline_allocation=result["baseline_allocation"].map(_public_count),
                )
            )
            scenario_grid.append(
                {
                    "budget_fraction": fraction,
                    "equity_floor": equity,
                    "allocations": allocation_rows,
                }
            )
            if equity == 0.25:
                curve.append(
                    {
                        "budget_fraction": fraction,
                        "budget_bdt": round(budget, 2),
                        "optimized_fill_pct": round(optimized_fill * 100, 1),
                        "baseline_fill_pct": round(baseline_fill * 100, 1),
                        "optimized_equity_pct": round(equity_index(coverage), 1),
                        "baseline_equity_pct": round(equity_index(baseline_coverage), 1),
                    }
                )
            if fraction == 0.60 and equity == 0.25:
                default_allocations = result
    return scenario_grid, curve, default_allocations


def export_dashboard_data(
    gold_csv: str | Path | None = None,
    output_dir: str | Path | None = None,
    district_ref_csv: str | Path | None = None,
    facility_registry_csv: str | Path | None = None,
    boundaries_geojson: str | Path | None = None,
    stock_csv: str | Path | None = None,
    dq_report_json: str | Path | None = None,
):
    gold_csv = Path(gold_csv) if gold_csv else ROOT / "pipeline" / "gold" / "gold_weekly_cases.csv"
    output_dir = Path(output_dir) if output_dir else OUT_DIR
    district_ref_csv = Path(district_ref_csv) if district_ref_csv else ROOT / "data_ref" / "district_ref.csv"
    facility_registry_csv = (
        Path(facility_registry_csv)
        if facility_registry_csv
        else ROOT / "data_raw" / "synthetic" / "facility_registry.csv"
    )
    stock_csv = (
        Path(stock_csv)
        if stock_csv
        else ROOT / "data_raw" / "synthetic" / "facility_stock_snapshot.csv"
    )
    dq_report_json = (
        Path(dq_report_json)
        if dq_report_json
        else ROOT / "pipeline" / "quality" / "dq_report.json"
    )
    boundaries_geojson = (
        Path(boundaries_geojson) if boundaries_geojson else ROOT / "data_raw" / "districts.geojson"
    )
    for input_path in (gold_csv, district_ref_csv, facility_registry_csv, boundaries_geojson):
        if not input_path.is_file():
            raise FileNotFoundError(f"Dashboard input not found: {input_path}")

    gold = pd.read_csv(gold_csv)
    missing = REQUIRED_GOLD_COLUMNS - set(gold.columns)
    if missing:
        raise ValueError(f"Gold mart is missing required columns: {', '.join(sorted(missing))}")

    district_ref = pd.read_csv(district_ref_csv)
    ref_columns = {"district_id", "district", "division", "lat", "lon", "population"}
    if not ref_columns.issubset(district_ref.columns):
        raise ValueError(f"District reference must contain {sorted(ref_columns)}.")
    if district_ref.duplicated(["district", "division"]).any():
        raise ValueError("District reference must have unique district and division pairs.")

    gold["week_start"] = pd.to_datetime(gold["week_start"], errors="raise").dt.normalize()
    gold["cases"] = pd.to_numeric(gold["cases"], errors="raise").clip(lower=0)
    gold["disease"] = gold["disease"].astype(str).str.lower()
    weekly = gold.merge(
        district_ref[["district_id", "district", "division", "lat", "lon", "population"]],
        on=["district", "division"],
        how="left",
        validate="many_to_one",
        suffixes=("", "_ref"),
    )
    if weekly["district_id"].isna().any():
        missing_districts = sorted(weekly.loc[weekly["district_id"].isna(), "district"].unique())
        raise ValueError(f"Gold mart districts are missing from the district reference: {missing_districts}")

    if "population_ref" in weekly:
        weekly["population"] = weekly["population_ref"].fillna(weekly["population"])
    weekly = weekly.drop(columns=["population_ref"], errors="ignore")
    latest_date = weekly["week_start"].max()
    stock, synthetic_stock = _read_stock_snapshot(
        facility_registry_csv,
        stock_csv,
        latest_date.date().isoformat(),
    )
    facilities = pd.read_csv(facility_registry_csv)
    facilities["beds"] = pd.to_numeric(facilities["beds"], errors="raise").clip(lower=0)
    stock = stock.merge(
        facilities[["facility_id", "beds"]],
        on="facility_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_registry"),
    )
    if stock["district_id"].isna().any():
        raise ValueError("Stock snapshot contains facilities without a district.")

    latest_rows = weekly[weekly["week_start"] == latest_date]
    weekly_with_ref = weekly.groupby(
        ["district_id", "district", "division", "disease", "week_start"], as_index=False
    )["cases"].sum()
    weekly_with_ref = weekly_with_ref.sort_values("week_start")

    district_weekly_records = []
    district_forecast = []
    raw_district_forecast = []
    for (district_id, district, division, disease), group in weekly_with_ref.groupby(
        ["district_id", "district", "division", "disease"], sort=True
    ):
        for record in group.to_dict(orient="records"):
            cases = float(record["cases"])
            district_weekly_records.append(
                {
                    "district_id": district_id,
                    "district": district,
                    "division": division,
                    "disease": disease,
                    "week_start": record["week_start"].date().isoformat(),
                    "cases": _public_count(cases),
                    "suppressed": 0 < round(cases) < SMALL_CELL_THRESHOLD,
                }
            )
        for item in _weekly_forecast(group, [], ()):
            estimate = item["forecast_cases"]
            lower = item["lower_80"]
            upper = item["upper_80"]
            raw_district_forecast.append(
                {
                    "district_id": district_id,
                    "disease": disease,
                    "week_start": item["week_start"],
                    "forecast_cases": estimate,
                }
            )
            suppressed = 0 < estimate < SMALL_CELL_THRESHOLD
            district_forecast.append(
                {
                    "district_id": district_id,
                    "district": district,
                    "division": division,
                    "disease": disease,
                    "week_start": item["week_start"],
                    "forecast_cases": None if suppressed else _public_count(estimate),
                    "lower_80": None if 0 < lower < SMALL_CELL_THRESHOLD else _public_count(lower),
                    "upper_80": None if 0 < upper < SMALL_CELL_THRESHOLD else _public_count(upper),
                    "suppressed": suppressed,
                }
            )

    published_weekly = pd.DataFrame(district_weekly_records)
    diseases = sorted(weekly["disease"].unique().tolist())
    districts = district_ref[["district_id", "district", "division", "lat", "lon", "population"]].copy()
    districts["district_id"] = districts["district_id"].astype(str)
    districts = districts.sort_values("district_id").reset_index(drop=True)
    weeks = sorted(published_weekly["week_start"].unique().tolist())
    district_indexes = {district_id: index for index, district_id in enumerate(districts["district_id"])}
    disease_indexes = {disease: index for index, disease in enumerate(diseases)}
    week_indexes = {week: index for index, week in enumerate(weeks)}
    district_weekly = [
        [
            district_indexes[row["district_id"]],
            disease_indexes[row["disease"]],
            week_indexes[row["week_start"]],
            row["cases"],
        ]
        for row in district_weekly_records
    ]
    national_trend = (
        published_weekly.dropna(subset=["cases"])
        .groupby(["week_start", "disease"], as_index=False)["cases"]
        .sum()
        .sort_values(["week_start", "disease"])
    )
    national_trend["partial"] = True
    national_trend["cases"] = national_trend["cases"].map(_public_count)

    forecast_frame = pd.DataFrame(district_forecast)
    visible_forecast = forecast_frame.dropna(subset=["forecast_cases"])
    forecast = (
        visible_forecast.groupby(["week_start", "disease"], as_index=False)
        .agg(
            forecast_cases=("forecast_cases", "sum"),
            lower_80=("lower_80", "sum"),
            upper_80=("upper_80", "sum"),
        )
        .sort_values(["week_start", "disease"])
    )
    forecast["partial"] = True
    for column in ("forecast_cases", "lower_80", "upper_80"):
        forecast[column] = forecast[column].map(_public_count)
    forecast_records = _records(forecast)

    seasonal_source = published_weekly.dropna(subset=["cases"]).copy()
    seasonal_dates = pd.to_datetime(seasonal_source["week_start"])
    seasonal_source["epi_year"] = seasonal_dates.dt.isocalendar().year.astype(int)
    seasonal_source["epi_week"] = seasonal_dates.dt.isocalendar().week.astype(int)
    seasonality = (
        seasonal_source.groupby(["disease", "epi_year", "epi_week"], as_index=False)["cases"]
        .sum()
        .sort_values(["epi_year", "epi_week", "disease"])
    )
    seasonality["suppressed"] = seasonality["cases"].lt(SMALL_CELL_THRESHOLD)
    seasonality["cases"] = seasonality["cases"].map(_public_count)
    preparedness = seasonal_calendar(
        weekly.rename(columns={"week_start": "date"})[
            ["date", "division", "disease", "cases"]
        ]
    )
    preparedness["suppressed"] = preparedness["expected_cases"].between(
        0, SMALL_CELL_THRESHOLD, inclusive="neither"
    )
    preparedness["expected_cases"] = preparedness["expected_cases"].map(_public_count)
    preparedness_records = _records(
        preparedness.drop(columns=["seasonal_component"])
    )

    latest_district = latest_rows.groupby(
        ["district_id", "district", "division", "lat", "lon", "population", "disease"],
        as_index=False,
    )["cases"].sum()
    latest_district["incidence_per_100k"] = (
        latest_district["cases"] * 100_000 / latest_district["population"].clip(lower=1)
    ).round(2)
    latest_district["suppressed"] = latest_district["cases"].round().between(
        1, SMALL_CELL_THRESHOLD - 1
    )
    latest_district["cases"] = latest_district["cases"].map(_public_count)
    latest_district.loc[latest_district["suppressed"], "incidence_per_100k"] = None
    latest_district["week_start"] = latest_date.date().isoformat()

    alert_candidates = _build_alerts(weekly_with_ref)
    if dq_report_json.is_file():
        dq_report = json.loads(dq_report_json.read_text(encoding="utf-8"))
        required_report_fields = {"generated_at_utc", "rows_in", "rows_out", "rows_quarantined", "rules"}
        missing_report_fields = required_report_fields - set(dq_report)
        if missing_report_fields:
            raise ValueError(
                f"DQ report is missing required fields: {sorted(missing_report_fields)}"
            )
        if not isinstance(dq_report["rules"], list):
            raise ValueError("DQ report rules must be a list.")
        dq_report_payload = {"available": True, **dq_report}
    else:
        dq_report_payload = {
            "available": False,
            "message": "No quality report exists yet. Run the Silver quality stage to create one.",
        }
    resource_demand = _build_resource_demand(
        pd.DataFrame(raw_district_forecast),
        district_ref,
        facilities,
        stock,
    )
    max_budget = sum(
        float(resource_demand.loc[resource_demand["resource"] == resource, "demand"].sum())
        * unit_cost
        for resource, unit_cost in RESOURCE_COSTS.items()
    )
    scenario_grid, allocation_curve, default_allocations = _solve_allocation_scenarios(
        resource_demand,
        max_budget,
    )
    if default_allocations.empty:
        raise RuntimeError("Resource allocation scenarios produced no default plan.")
    allocation_records = scenario_grid[
        next(
            index for index, scenario in enumerate(scenario_grid)
            if scenario["budget_fraction"] == 0.60 and scenario["equity_floor"] == 0.25
        )
    ]["allocations"]

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_simplified_boundaries(boundaries_geojson, output_dir / "district_boundaries.geojson")
    suppressed_cells = int(published_weekly["suppressed"].sum())
    summary = {
        "row_count": int(len(gold)),
        "visible_case_totals": {
            disease: int(total)
            for disease, total in national_trend.groupby("disease")["cases"].sum().items()
            if total >= SMALL_CELL_THRESHOLD
        },
        "latest_week_date": latest_date.date().isoformat(),
        "data_freshness_date": str(stock["snapshot_date"].max()),
        "diseases": diseases,
        "districts": _records(districts),
        "weeks": weeks,
        "district_weekly": district_weekly,
        "weekly_trend": _records(national_trend),
        "district_forecast": district_forecast,
        "forecast": forecast_records,
        "seasonality": _records(seasonality),
        "preparedness_calendar": preparedness_records,
        "data_quality_report": dq_report_payload,
        "alert_candidates": alert_candidates,
        "alerts": [
            candidate
            for candidate in alert_candidates
            if candidate["z_score"] >= 2.0 and candidate["growth_pct"] >= 50.0
        ],
        "allocations": allocation_records,
        "allocation_scenarios": scenario_grid,
        "allocation_curve": allocation_curve,
        "resource_labels": RESOURCE_LABELS,
        "assumptions": {
            "forecast_horizon_weeks": FORECAST_HORIZON_WEEKS,
            "forecast_method": "District-level mean of the latest four weekly counts; interval uses the prior eight-week standard deviation. This is a heuristic baseline, not a validated forecast model.",
            "alert_rule": "Recent four-week mean is at least 50% above the preceding eight-week mean and exceeds the selected baseline standard-deviation threshold.",
            "admission_rate": ADMISSION_RATE,
            "average_stay_days": AVERAGE_STAY_DAYS,
            "test_kits_per_forecast_case": TEST_KITS_PER_CASE,
            "saline_units_per_expected_admission": SALINE_UNITS_PER_ADMISSION,
            "resource_cost_bdt": RESOURCE_COSTS,
            "small_cell_threshold": SMALL_CELL_THRESHOLD,
            "small_cells_suppressed": suppressed_cells,
            "visible_case_totals_are_partial": True,
            "stock_source": "Synthetic planning scenario generated from facility capacity; replace with the course S6 stock feed before treating this as a real stock position."
            if synthetic_stock
            else "Facility-level stock snapshot supplied as an input.",
            "stock_assumptions": "Synthetic facility stock quantities are seeded per facility from registered bed capacity; they are for dashboard and optimizer testing only.",
            "demand_conversion": "Beds = admissions × average stay ÷ 28 days; IV saline = admissions × 2; NS1 kits = dengue forecast cases; ORS = diarrhoea forecast × 1.2; antimalarials = malaria forecast × 0.9.",
            "allocation_method": "PuLP/CBC minimizes severity-weighted unmet demand across resource pools subject to current national stock and budget. The equity floor requires each district to receive at least alpha × its population share of allocations for each resource.",
            "allocation_default_budget_fraction": 0.60,
            "allocation_default_equity_floor": 0.25,
            "alert_candidates_have_suppressed_small_cells": True,
        },
    }
    output_path = output_dir / "dashboard_summary.json"
    output_path.write_text(json.dumps(summary, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    total_output_bytes = output_path.stat().st_size + (output_dir / "district_boundaries.geojson").stat().st_size
    if total_output_bytes > 10 * 1024 * 1024:
        raise ValueError(
            f"Aggregated dashboard assets exceed the 10 MB VIS-4 limit: {total_output_bytes:,} bytes."
        )
    return summary


if __name__ == "__main__":
    summary = export_dashboard_data()
    print(
        f"Dashboard data exported: {summary['row_count']:,} weekly rows, "
        f"{len(summary['districts'])} district/disease rows, "
        f"{len(summary['alerts'])} outbreak alerts."
    )
