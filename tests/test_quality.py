import json
import pandas as pd
import pytest

from pipeline.quality.quality_rules import (
    apply_quality_and_privacy_checks,
    hash_patient_id,
)


@pytest.fixture(autouse=True)
def configured_pii_salt(monkeypatch):
    monkeypatch.setenv("PII_SALT", "test-salt-for-unit-fixtures-8f2c4d6a9b1e7c3a")


@pytest.fixture
def local_references(tmp_path):
    registry_path = tmp_path / "facility_registry.csv"
    district_path = tmp_path / "district_ref.csv"
    pd.DataFrame(
        [
            {"facility_id": "F0001", "district_id": "BD01"},
            {"facility_id": "F0002", "district_id": "BD02"},
        ]
    ).to_csv(registry_path, index=False)
    pd.DataFrame(
        [
            {"district_id": "BD01", "district": "District One"},
            {"district_id": "BD02", "district": "District Two"},
        ]
    ).to_csv(district_path, index=False)
    return registry_path, district_path


def valid_encounter(**overrides):
    row = {
        "encounter_id": "E1",
        "patient_id": "PID_001",
        "facility_id": "F0001",
        "onset_date": "2025-01-02",
        "report_date": "2025-01-03",
        "age_years": 25,
        "sex": "F",
        "icd10_code": "A90",
        "lab_confirmed": 1,
        "admitted": 0,
        "outcome": "recovered",
    }
    row.update(overrides)
    return row


def run_load(tmp_path, rows, local_references, **kwargs):
    input_dir = tmp_path / "landing"
    input_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(input_dir / "encounters.csv", index=False)
    registry_path, district_path = local_references
    clean_path = tmp_path / "silver" / "clean.parquet"
    quarantine_path = tmp_path / "quality" / "quarantine.csv"
    report_path = tmp_path / "quality" / "load_report.json"
    result = apply_quality_and_privacy_checks(
        input_dir,
        clean_path,
        quarantine_path,
        report_path,
        registry_path,
        district_path,
        **kwargs,
    )
    return result, clean_path, quarantine_path, report_path


def test_valid_record_is_hashed_and_district_is_derived(tmp_path, local_references):
    (clean, quarantine), clean_path, _, report_path = run_load(
        tmp_path, [valid_encounter()], local_references
    )

    assert len(clean) == 1
    assert quarantine.empty
    assert clean["patient_id"].iat[0] == hash_patient_id("PID_001")
    assert clean["age_group"].iat[0] == "19-45"
    assert clean["district_id"].iat[0] == "BD01"
    assert clean["district"].iat[0] == "District One"
    assert clean_path.is_file()
    assert report_path.is_file()
    assert report_path.with_suffix(".csv").is_file()


@pytest.mark.parametrize(
    ("updates", "reason_code"),
    [
        ({"patient_id": ""}, "DQ_REQUIRED_VALUES_NOT_NULL"),
        ({"age_years": 130}, "DQ_AGE_IN_RANGE"),
        ({"sex": "X"}, "DQ_SEX_ALLOWED"),
        ({"onset_date": "not-a-date"}, "DQ_ONSET_DATE_VALID"),
        ({"report_date": "not-a-date"}, "DQ_REPORT_DATE_VALID"),
        ({"onset_date": "2025-01-04", "report_date": "2025-01-03"}, "DQ_DATE_ORDER"),
        ({"report_date": "2025-01-24"}, "DQ_REPORT_DELAY_0_21_DAYS"),
        ({"icd10_code": "Z00"}, "DQ_ICD10_ALLOWED"),
        ({"lab_confirmed": 2}, "DQ_LAB_CONFIRMED_BINARY"),
        ({"admitted": -1}, "DQ_ADMITTED_BINARY"),
        ({"outcome": "severe"}, "DQ_OUTCOME_ALLOWED"),
        ({"facility_id": "F9999"}, "DQ_FACILITY_REGISTERED"),
        ({"district": "Wrong District"}, "DQ_GEOGRAPHY_VALID"),
        ({"onset_date": "1899-01-01", "report_date": "1899-01-02"}, "DQ_REASONABLE_VALUES"),
    ],
)
def test_each_row_quality_rule_quarantines_with_stable_reason_code(
    tmp_path, local_references, updates, reason_code
):
    (clean, quarantine), _, _, _ = run_load(
        tmp_path, [valid_encounter(**updates)], local_references
    )

    assert clean.empty
    assert len(quarantine) == 1
    assert reason_code in quarantine["dq_reason_codes"].iat[0].split("|")
    assert quarantine["dq_reason_code"].iat[0] == quarantine["dq_reason_codes"].iat[0].split("|")[0]


def test_schema_rule_fails_explicitly_when_required_column_is_missing(tmp_path, local_references):
    record = valid_encounter()
    del record["admitted"]
    with pytest.raises(ValueError, match="missing required columns.*admitted"):
        run_load(tmp_path, [record], local_references)


def test_duplicate_encounter_ids_quarantine_every_duplicate(tmp_path, local_references):
    rows = [
        valid_encounter(),
        valid_encounter(patient_id="PID_002", facility_id="F0002"),
    ]
    (clean, quarantine), _, _, _ = run_load(tmp_path, rows, local_references)

    assert clean.empty
    assert len(quarantine) == 2
    assert quarantine["dq_reason_codes"].str.contains("DQ_ENCOUNTER_ID_UNIQUE").all()


def test_report_contains_per_rule_counts_and_load_totals(tmp_path, local_references):
    rows = [valid_encounter(), valid_encounter(encounter_id="E2", age_years=150)]
    (_, quarantine), _, _, report_path = run_load(tmp_path, rows, local_references)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    csv_report = pd.read_csv(report_path.with_suffix(".csv"))
    age_rule = next(rule for rule in report["rules"] if rule["rule_code"] == "DQ_AGE_IN_RANGE")

    assert report["rows_in"] == 2
    assert report["rows_out"] == 1
    assert report["rows_quarantined"] == len(quarantine) == 1
    assert age_rule["pass_count"] == 1
    assert age_rule["fail_count"] == 1
    assert len(report["rules"]) >= 12
    assert {"pass_count", "fail_count", "rows_in", "rows_out"}.issubset(csv_report.columns)


def test_facility_and_geography_checks_are_disabled_without_local_registry(
    tmp_path, local_references
):
    missing_registry = tmp_path / "not-installed.csv"
    missing_districts = tmp_path / "not-installed-districts.csv"
    (clean, quarantine), _, _, report_path = run_load(
        tmp_path,
        [valid_encounter(facility_id="unmapped-but-no-registry")],
        (missing_registry, missing_districts),
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    states = {rule["rule_code"]: rule["enabled"] for rule in report["rules"]}

    assert len(clean) == 1
    assert quarantine.empty
    assert states["DQ_FACILITY_REGISTERED"] is False
    assert states["DQ_GEOGRAPHY_VALID"] is False


def test_supplied_district_id_must_match_facility_mapping(tmp_path, local_references):
    (_, quarantine), _, _, _ = run_load(
        tmp_path, [valid_encounter(district_id="BD02")], local_references
    )

    assert "DQ_GEOGRAPHY_VALID" in quarantine["dq_reason_codes"].iat[0].split("|")


def test_hash_is_reproducible_for_same_salt_and_identifier(monkeypatch):
    monkeypatch.setenv("PII_SALT", "test-salt-for-unit-fixtures-8f2c4d6a9b1e7c3a")
    first = hash_patient_id("PID_123")
    second = hash_patient_id(" PID_123 ")

    assert first == second
    assert len(first) == 64


@pytest.mark.parametrize(
    ("salt", "message"),
    [
        (None, "PII_SALT is required"),
        ("too-short", "malformed or weak"),
        ("change-me-to-a-long-random-string", "malformed or weak"),
        ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "malformed or weak"),
        ("valid-but-padded-secret-8f2c4d6a9b1e7c3a ", "malformed or weak"),
    ],
)
def test_missing_weak_or_malformed_salt_fails_without_fallback(monkeypatch, salt, message):
    if salt is None:
        monkeypatch.delenv("PII_SALT", raising=False)
    else:
        monkeypatch.setenv("PII_SALT", salt)

    with pytest.raises(ValueError, match=message):
        hash_patient_id("PID_123")


def test_missing_salt_stops_load_before_writing_outputs(
    tmp_path, local_references, monkeypatch
):
    monkeypatch.delenv("PII_SALT", raising=False)
    input_dir = tmp_path / "landing"
    input_dir.mkdir()
    pd.DataFrame([valid_encounter()]).to_csv(input_dir / "encounters.csv", index=False)
    clean_path = tmp_path / "clean.parquet"
    with pytest.raises(ValueError, match="PII_SALT is required"):
        apply_quality_and_privacy_checks(
            input_dir,
            clean_path,
            tmp_path / "quarantine.csv",
            None,
            *local_references,
        )
    assert not clean_path.exists()
