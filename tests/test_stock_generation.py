import pandas as pd

from generator.generate_stock import DEFAULT_OUTPUT, generate_stock_snapshot
from pipeline.bronze.ingest_bronze import DEFAULT_INPUTS, classify


def test_synthetic_stock_is_generated_once_and_classified_for_bronze(tmp_path):
    facilities_path = tmp_path / "facilities.csv"
    pd.DataFrame(
        [
            {"facility_id": "F001", "district_id": "BD1004", "beds": 20},
            {"facility_id": "F002", "district_id": "BD1006", "beds": 10},
        ]
    ).to_csv(facilities_path, index=False)
    stock_path = tmp_path / "stock.csv"

    first = generate_stock_snapshot(
        facilities_path, stock_path, snapshot_date="2025-12-31", seed=1
    )
    second = generate_stock_snapshot(
        facilities_path, stock_path, snapshot_date="2026-01-01", seed=2
    )

    pd.testing.assert_frame_equal(first, second, check_dtype=False)
    assert len(second) == 2
    assert second["stock_source"].eq("synthetic_planning_scenario").all()
    assert set(second["snapshot_date"]) == {"2025-12-31"}
    assert classify(stock_path) == "stock"
    assert DEFAULT_OUTPUT in DEFAULT_INPUTS


def test_existing_stock_input_is_preserved(tmp_path):
    facilities_path = tmp_path / "facilities.csv"
    pd.DataFrame(
        [{"facility_id": "F001", "district_id": "BD1004", "beds": 20}]
    ).to_csv(facilities_path, index=False)
    stock_path = tmp_path / "stock.csv"
    stock_path.write_text(
        "facility_id,district_id,snapshot_date,beds_occupied,iv_saline,ns1_kits,ors,antimalarials\n"
        "F001,BD1004,2025-12-31,5,10,4,20,3\n",
        encoding="utf-8",
    )

    result = generate_stock_snapshot(facilities_path, stock_path)

    assert result.loc[0, "iv_saline"] == 10
    assert "stock_source" not in result.columns
