import json

import pandas as pd

from serving.export_serving_data import export_dashboard_data


def test_export_contains_five_dashboard_views_and_resource_estimates(tmp_path):
    dates = pd.date_range("2025-01-06", periods=12, freq="7D")
    gold = pd.DataFrame(
        {
            "week_start": dates,
            "division": ["Barisal"] * len(dates),
            "district": ["Barguna"] * len(dates),
            "disease": ["dengue"] * len(dates),
            "cases": [5] * 8 + [20] * 4,
        }
    )
    gold = pd.concat(
        [
            gold,
            pd.DataFrame(
                [{
                    "week_start": dates[-1],
                    "division": "Barisal",
                    "district": "Barguna",
                    "disease": "malaria",
                    "cases": 3,
                }]
            ),
        ],
        ignore_index=True,
    )
    gold_path = tmp_path / "gold.csv"
    gold.to_csv(gold_path, index=False)
    district_ref_path = tmp_path / "district_ref.csv"
    pd.DataFrame(
        [{
            "district_id": "BD1004",
            "district": "Barguna",
            "division": "Barisal",
            "lat": 22.14,
            "lon": 90.12,
            "population": 100_000,
        }]
    ).to_csv(district_ref_path, index=False)
    facilities_path = tmp_path / "facilities.csv"
    pd.DataFrame([{"facility_id": "F001", "district_id": "BD1004", "beds": 10}]).to_csv(
        facilities_path, index=False
    )
    stock_path = tmp_path / "stock.csv"
    pd.DataFrame(
        [{
            "facility_id": "F001",
            "district_id": "BD1004",
            "snapshot_date": dates[-1].date().isoformat(),
            "beds_occupied": 5,
            "iv_saline": 60,
            "ns1_kits": 30,
            "ors": 80,
            "antimalarials": 20,
            "stock_source": "test_fixture",
        }]
    ).to_csv(stock_path, index=False)
    boundaries_path = tmp_path / "districts.geojson"
    boundaries_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"adm2_pcode": "BD1004", "adm2_en": "Barguna"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    dq_report_path = tmp_path / "dq_report.json"
    dq_report_path.write_text(
        json.dumps(
            {
                "generated_at_utc": "2025-01-01T00:00:00+00:00",
                "rows_in": 10,
                "rows_out": 8,
                "rows_quarantined": 2,
                "rules": [
                    {
                        "rule_code": "DQ_TEST",
                        "description": "Test rule",
                        "enabled": True,
                        "pass_count": 8,
                        "fail_count": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = export_dashboard_data(
        gold_csv=gold_path,
        output_dir=tmp_path / "serving",
        district_ref_csv=district_ref_path,
        facility_registry_csv=facilities_path,
        boundaries_geojson=boundaries_path,
        stock_csv=stock_path,
        dq_report_json=dq_report_path,
    )

    assert summary["row_count"] == 13
    assert summary["latest_week_date"] == dates[-1].date().isoformat()
    assert len(summary["district_weekly"]) == 13
    assert len(summary["forecast"]) == 4
    assert all({"epi_year", "epi_week", "cases"}.issubset(row) for row in summary["seasonality"])
    assert isinstance(summary["preparedness_calendar"], list)
    assert summary["data_quality_report"]["available"] is True
    assert summary["data_quality_report"]["rows_quarantined"] == 2
    assert summary["data_quality_report"]["rules"][0]["rule_code"] == "DQ_TEST"
    assert summary["allocation_scenarios"]
    assert len(summary["allocation_curve"]) == 5
    assert len({row["optimized_fill_pct"] for row in summary["allocation_curve"]}) > 1
    assert summary["assumptions"]["visible_case_totals_are_partial"] is True
    malaria_index = summary["diseases"].index("malaria")
    suppressed = next(row for row in summary["district_weekly"] if row[1] == malaria_index)
    assert suppressed[3] is None
    dengue_index = summary["diseases"].index("dengue")
    latest_week_index = summary["weeks"].index(dates[-1].date().isoformat())
    assert [0, dengue_index, latest_week_index, 20] in summary["district_weekly"]
    assert summary["alert_candidates"][0]["recent_cases"] >= 5
    allocation = next(row for row in summary["allocations"] if row["resource"] == "iv_saline")
    assert allocation["current_stock"] >= 5
    assert allocation["allocated"] is None or allocation["allocated"] >= 5
    assert allocation["baseline_allocation"] is None or allocation["baseline_allocation"] >= 5

    output_dir = tmp_path / "serving"
    summary_path = output_dir / "dashboard_summary.json"
    exported = json.loads(summary_path.read_text(encoding="utf-8"))
    assert exported["assumptions"]["stock_source"].startswith("Facility-level")
    assert summary_path.stat().st_size + (output_dir / "district_boundaries.geojson").stat().st_size < 10 * 1024 * 1024
    for row in summary["district_weekly"]:
        assert row[3] is None or row[3] == 0 or row[3] >= 5
    for row in summary["district_forecast"]:
        for field in ("forecast_cases", "lower_80", "upper_80"):
            value = row[field]
            assert value is None or value == 0 or value >= 5
    for row in summary["preparedness_calendar"]:
        value = row["expected_cases"]
        assert value is None or value == 0 or value >= 5
    for scenario in summary["allocation_scenarios"]:
        for row in scenario["allocations"]:
            for field in ("demand", "current_stock", "allocated", "unmet", "baseline_allocation"):
                value = row[field]
                assert value is None or value == 0 or value >= 5
    boundaries = json.loads(
        (tmp_path / "serving" / "district_boundaries.geojson").read_text(encoding="utf-8")
    )
    assert boundaries["type"] == "FeatureCollection"
    exterior = boundaries["features"][0]["geometry"]["coordinates"][0]
    signed_area = sum(
        exterior[index][0] * exterior[index + 1][1]
        - exterior[index + 1][0] * exterior[index][1]
        for index in range(len(exterior) - 1)
    )
    assert signed_area < 0

    model_forecast_path = tmp_path / "weekly_forecast.csv"
    pd.DataFrame(
        [
            {
                "district_id": "BD1004",
                "disease": disease,
                "origin_week": dates[-1].date().isoformat(),
                "forecast_week": (dates[-1] + pd.Timedelta(weeks=horizon)).date().isoformat(),
                "horizon": horizon,
                "forecast_cases": 10 + horizon,
                "lower_80": 8 + horizon,
                "upper_80": 12 + horizon,
            }
            for disease in ("dengue", "malaria")
            for horizon in range(1, 5)
        ]
    ).to_csv(model_forecast_path, index=False)
    candidate_path = tmp_path / "alert_candidates.csv"
    pd.DataFrame(
        [
            {
                "district_id": "BD1004",
                "district": "Barguna",
                "division": "Barisal",
                "disease": "dengue",
                "recent_cases_per_week": 12,
                "baseline_cases_per_week": 6,
                "growth_pct": 100,
                "z_score": 4.2,
                "recent_cases": 48,
                "latest_week": dates[-1].date().isoformat(),
            }
        ]
    ).to_csv(candidate_path, index=False)
    model_summary = export_dashboard_data(
        gold_csv=gold_path,
        output_dir=tmp_path / "model-serving",
        district_ref_csv=district_ref_path,
        facility_registry_csv=facilities_path,
        boundaries_geojson=boundaries_path,
        stock_csv=stock_path,
        dq_report_json=dq_report_path,
        forecast_csv=model_forecast_path,
        alert_candidates_csv=candidate_path,
    )
    assert model_summary["assumptions"]["forecast_model_enabled"] is True
    assert "Spark ridge model" in model_summary["assumptions"]["forecast_method"]
    assert model_summary["forecast"][0]["forecast_cases"] == 11
    assert model_summary["alert_candidates"][0]["z_score"] == 4.2
