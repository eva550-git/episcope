"""Quality and privacy controls for raw encounter data.

Patient identifiers are hashed with the deployment-provided ``PII_SALT``. Records
that fail one or more named checks are written to quarantine with stable reason
codes, while a per-load JSON and CSV report records each rule's pass/fail counts.
"""

import argparse
import hashlib
import json
import math
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "data_raw" / "landing" / "encounters"
DEFAULT_CLEAN = ROOT / "pipeline" / "silver" / "clean_encounters.parquet"
DEFAULT_QUARANTINE = ROOT / "pipeline" / "quality" / "quarantined_records.csv"
DEFAULT_REPORT = ROOT / "pipeline" / "quality" / "dq_report.json"
DEFAULT_FACILITY_REGISTRY = ROOT / "data_raw" / "synthetic" / "facility_registry.csv"
DEFAULT_DISTRICT_REFERENCE = ROOT / "data_ref" / "district_ref.csv"

# These are the ICD-10 values emitted by generator/generate_encounters.py.
GENERATOR_ICD10_CODES = frozenset(
    {
        "A90", "A91", "A09", "A00", "J11", "J10", "B54", "B50", "B51", "B52",
        "I10", "E11", "J18", "K29", "N39", "R50", "A01", "J45", "K52", "L03",
        "M54", "S06",
    }
)
ALLOWED_SEX = frozenset({"m", "f", "u"})
ALLOWED_OUTCOMES = frozenset({"recovered", "referred", "died"})
REQUIRED_COLUMNS = (
    "encounter_id",
    "patient_id",
    "facility_id",
    "onset_date",
    "report_date",
    "age_years",
    "sex",
    "icd10_code",
    "lab_confirmed",
    "admitted",
    "outcome",
)

# Rule order is stable and is also the order used in quarantine reason codes.
RULE_DESCRIPTIONS = {
    "DQ_SCHEMA_REQUIRED_COLUMNS": "Required generator schema columns are present",
    "DQ_REQUIRED_VALUES_NOT_NULL": "Required values are non-null and non-blank",
    "DQ_AGE_IN_RANGE": "Age is numeric and between 0 and 120 years",
    "DQ_SEX_ALLOWED": "Sex is one of M, F, or U",
    "DQ_ONSET_DATE_VALID": "Onset date parses as a valid date",
    "DQ_REPORT_DATE_VALID": "Report date parses as a valid date",
    "DQ_DATE_ORDER": "Onset date is not after report date",
    "DQ_REPORT_DELAY_0_21_DAYS": "Report delay is between 0 and 21 days",
    "DQ_ICD10_ALLOWED": "ICD-10 code belongs to the encounter generator's code set",
    "DQ_LAB_CONFIRMED_BINARY": "Lab confirmation is binary (0 or 1)",
    "DQ_ADMITTED_BINARY": "Admission is binary (0 or 1)",
    "DQ_OUTCOME_ALLOWED": "Outcome is recovered, referred, or died",
    "DQ_ENCOUNTER_ID_UNIQUE": "Encounter ID is unique within the load",
    "DQ_FACILITY_REGISTERED": "Facility ID exists in the local registry when available",
    "DQ_GEOGRAPHY_VALID": "District agrees with facility mapping and local geography",
    "DQ_REASONABLE_VALUES": "Dates and numeric values are within reasonable bounds",
}


def _get_hash_salt():
    """Return the configured PII salt or fail closed with a useful message."""
    salt = os.environ.get("PII_SALT")
    if salt is None or not salt.strip():
        raise ValueError(
            "PII_SALT is required; set it in the process environment before "
            "hashing patient identifiers. No source-code fallback is used."
        )
    if salt != salt.strip() or len(salt) < 32:
        raise ValueError(
            "PII_SALT is malformed or weak: provide at least 32 non-padded "
            "characters of random secret material."
        )
    lowered = salt.lower()
    if (
        any(ord(character) < 32 or ord(character) == 127 for character in salt)
        or any(token in lowered for token in ("change-me", "changeme", "placeholder", "replace-me", "example"))
        or len(set(salt)) < 8
        or len(set(salt[index:index + 4] for index in range(max(0, len(salt) - 3)))) < 4
    ):
        raise ValueError(
            "PII_SALT is malformed or weak: use a non-placeholder, high-entropy "
            "secret with at least eight distinct characters."
        )
    entropy = -sum(
        (count / len(salt)) * math.log2(count / len(salt))
        for count in (salt.count(character) for character in set(salt))
    )
    if entropy < 3.0:
        raise ValueError(
            "PII_SALT is weak: use a high-entropy random secret (at least "
            "3 bits of character entropy)."
        )
    return salt.encode("utf-8")


def _hash_patient_id(value, salt):
    raw = "" if pd.isna(value) else str(value).strip()
    return hashlib.sha256(salt + raw.encode("utf-8")).hexdigest()


def hash_patient_id(value):
    """Hash one patient identifier using the required ``PII_SALT`` environment value."""
    return _hash_patient_id(value, _get_hash_salt())


def build_age_group(age):
    if pd.isna(age):
        return None
    try:
        age_value = float(age)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(age_value) or age_value < 0 or age_value > 120:
        return None
    if age_value <= 5:
        return "0-5"
    if age_value <= 18:
        return "6-18"
    if age_value <= 45:
        return "19-45"
    if age_value <= 65:
        return "46-65"
    return "65+"


def _read_reference(path, required_columns, description):
    path = Path(path)
    if not path.is_file():
        return None
    reference = pd.read_csv(path)
    missing = set(required_columns) - set(reference.columns)
    if missing:
        raise ValueError(
            f"{description} at {path} is missing columns: {sorted(missing)}"
        )
    return reference


def _load_geography(facility_registry_path, district_reference_path):
    registry = _read_reference(
        facility_registry_path, ("facility_id", "district_id"), "Facility registry"
    )
    district_reference = _read_reference(
        district_reference_path, ("district_id", "district"), "District reference"
    )
    facility_to_district = {}
    district_names = {}
    valid_district_ids = set()
    if registry is not None:
        registry = registry.dropna(subset=["facility_id", "district_id"]).copy()
        registry["facility_id"] = registry["facility_id"].astype(str).str.strip()
        registry["district_id"] = registry["district_id"].astype(str).str.strip()
        if registry["facility_id"].duplicated().any():
            raise ValueError("Facility registry contains duplicate facility_id values.")
        facility_to_district = dict(zip(registry.facility_id, registry.district_id))
    if district_reference is not None:
        district_reference = district_reference.dropna(
            subset=["district_id", "district"]
        ).copy()
        district_reference["district_id"] = (
            district_reference["district_id"].astype(str).str.strip()
        )
        if district_reference["district_id"].duplicated().any():
            raise ValueError("District reference contains duplicate district_id values.")
        district_names = dict(
            zip(district_reference.district_id, district_reference.district.astype(str).str.strip())
        )
        valid_district_ids = set(district_names)
    return facility_to_district, district_names, valid_district_ids


def _bool_series(values, index):
    return pd.Series(values, index=index, dtype=bool).fillna(False)


def _evaluate_rules(df, facility_to_district, district_names, valid_district_ids):
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Input data is missing required columns: {sorted(missing)}")

    index = df.index
    rules = {}
    rules["DQ_SCHEMA_REQUIRED_COLUMNS"] = pd.Series(True, index=index, dtype=bool)

    required_values_present = pd.Series(True, index=index, dtype=bool)
    for column in REQUIRED_COLUMNS:
        present = df[column].notna()
        if pd.api.types.is_object_dtype(df[column]) or pd.api.types.is_string_dtype(df[column]):
            present &= df[column].astype("string").str.strip().ne("").fillna(False)
        required_values_present &= present
    rules["DQ_REQUIRED_VALUES_NOT_NULL"] = required_values_present

    age = pd.to_numeric(df["age_years"], errors="coerce")
    age_valid = age.notna() & age.between(0, 120) & age.map(
        lambda value: math.isfinite(value) if pd.notna(value) else False
    )
    rules["DQ_AGE_IN_RANGE"] = _bool_series(age_valid, index)
    sex = df["sex"].astype("string").str.strip().str.lower()
    rules["DQ_SEX_ALLOWED"] = _bool_series(sex.isin(ALLOWED_SEX), index)

    onset = pd.to_datetime(df["onset_date"], errors="coerce")
    reported = pd.to_datetime(df["report_date"], errors="coerce")
    rules["DQ_ONSET_DATE_VALID"] = onset.notna()
    rules["DQ_REPORT_DATE_VALID"] = reported.notna()
    rules["DQ_DATE_ORDER"] = _bool_series(onset.notna() & reported.notna() & (onset <= reported), index)
    delay = (reported - onset).dt.total_seconds() / 86400
    rules["DQ_REPORT_DELAY_0_21_DAYS"] = _bool_series(
        onset.notna() & reported.notna() & delay.between(0, 21), index
    )

    icd = df["icd10_code"].astype("string").str.strip().str.upper()
    rules["DQ_ICD10_ALLOWED"] = _bool_series(icd.isin(GENERATOR_ICD10_CODES), index)
    for column, rule_id in (
        ("lab_confirmed", "DQ_LAB_CONFIRMED_BINARY"),
        ("admitted", "DQ_ADMITTED_BINARY"),
    ):
        binary = pd.to_numeric(df[column], errors="coerce")
        rules[rule_id] = _bool_series(binary.isin((0, 1)), index)
    outcome = df["outcome"].astype("string").str.strip().str.lower()
    rules["DQ_OUTCOME_ALLOWED"] = _bool_series(outcome.isin(ALLOWED_OUTCOMES), index)

    encounter_ids = df["encounter_id"].astype("string").str.strip()
    duplicate_ids = encounter_ids.duplicated(keep=False) | encounter_ids.isna()
    rules["DQ_ENCOUNTER_ID_UNIQUE"] = _bool_series(~duplicate_ids, index)

    facility_keys = df["facility_id"].astype("string").str.strip()
    if facility_to_district:
        facility_registered = facility_keys.isin(facility_to_district)
        rules["DQ_FACILITY_REGISTERED"] = _bool_series(facility_registered, index)
    else:
        rules["DQ_FACILITY_REGISTERED"] = None

    # Validate any supplied geography before adding authoritative facility-derived values.
    source_district = (
        df["district"].astype("string").str.strip().str.casefold()
        if "district" in df.columns else None
    )
    source_district_id = (
        df["district_id"].astype("string").str.strip()
        if "district_id" in df.columns else None
    )
    geography_available = bool(facility_to_district) or source_district is not None or source_district_id is not None
    if geography_available:
        if facility_to_district:
            derived_district_ids = facility_keys.map(facility_to_district)
            geography_valid = derived_district_ids.notna()
            if valid_district_ids:
                geography_valid &= derived_district_ids.isin(valid_district_ids)
            if source_district_id is not None:
                geography_valid &= (
                    source_district_id.notna()
                    & source_district_id.eq(derived_district_ids)
                )
            if source_district is not None:
                expected_names = derived_district_ids.map(district_names).astype("string").str.casefold()
                expected_ids = derived_district_ids.astype("string").str.casefold()
                geography_valid &= (
                    source_district.notna()
                    & (source_district.eq(expected_names) | source_district.eq(expected_ids))
                )
        else:
            geography_valid = pd.Series(True, index=index, dtype=bool)
            if source_district_id is not None:
                geography_valid &= source_district_id.notna()
                if valid_district_ids:
                    geography_valid &= source_district_id.isin(valid_district_ids)
            if source_district is not None:
                if district_names:
                    allowed_names = {name.casefold() for name in district_names.values()}
                    allowed_names |= {district_id.casefold() for district_id in district_names}
                    geography_valid &= source_district.isin(allowed_names)
                else:
                    geography_valid &= source_district.notna()
        rules["DQ_GEOGRAPHY_VALID"] = _bool_series(geography_valid, index)
    else:
        rules["DQ_GEOGRAPHY_VALID"] = None

    now = pd.Timestamp(date.today() + timedelta(days=1))
    reasonable = (
        age.notna()
        & age.between(0, 120)
        & onset.notna()
        & reported.notna()
        & onset.ge(pd.Timestamp("1900-01-01"))
        & reported.ge(pd.Timestamp("1900-01-01"))
        & onset.le(now)
        & reported.le(now)
    )
    rules["DQ_REASONABLE_VALUES"] = _bool_series(reasonable, index)

    return rules, onset, reported, facility_keys


def _prepare_geography(df, facility_to_district, district_names):
    prepared = df.copy()
    if not facility_to_district:
        return prepared
    facility_keys = prepared["facility_id"].astype("string").str.strip()
    district_ids = facility_keys.map(facility_to_district)
    prepared["district_id"] = district_ids
    if district_names:
        prepared["district"] = district_ids.map(district_names)
    return prepared


def _combine_rule_failures(rules, index):
    failures = pd.Series(False, index=index, dtype=bool)
    for passed in rules.values():
        if passed is not None:
            failures |= ~passed
    return failures


def validate_rules(df):
    """Return a per-row failure mask while retaining the legacy public helper."""
    facility_to_district, district_names, valid_district_ids = _load_geography(
        DEFAULT_FACILITY_REGISTRY, DEFAULT_DISTRICT_REFERENCE
    )
    rules, _, _, _ = _evaluate_rules(
        df, facility_to_district, district_names, valid_district_ids
    )
    return _combine_rule_failures(rules, df.index)


def _write_dq_report(report_path, rows_in, rows_out, rows_quarantined, rules):
    report_path = Path(report_path)
    csv_path = report_path.with_suffix(".csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rule_rows = []
    for rule_id, passed in rules.items():
        enabled = passed is not None
        pass_count = int(passed.sum()) if enabled else None
        fail_count = int((~passed).sum()) if enabled else None
        rule_rows.append(
            {
                "rule_code": rule_id,
                "description": RULE_DESCRIPTIONS[rule_id],
                "enabled": enabled,
                "pass_count": pass_count,
                "fail_count": fail_count,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_quarantined": rows_quarantined,
            }
        )
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_quarantined": rows_quarantined,
        "rules": rule_rows,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    pd.DataFrame(rule_rows).to_csv(csv_path, index=False)


def apply_quality_and_privacy_checks(
    input_dir=DEFAULT_INPUT,
    clean_path=DEFAULT_CLEAN,
    quarantine_path=DEFAULT_QUARANTINE,
    report_path=None,
    facility_registry_path=DEFAULT_FACILITY_REGISTRY,
    district_reference_path=DEFAULT_DISTRICT_REFERENCE,
):
    files = sorted(Path(input_dir).glob("*.csv"))
    if not files:
        print(f"No CSV files found in {input_dir}; skipping quality checks.")
        return None

    frames = [pd.read_csv(path) for path in files]
    df = pd.concat(frames, ignore_index=True)
    salt = _get_hash_salt()
    facility_to_district, district_names, valid_district_ids = _load_geography(
        facility_registry_path, district_reference_path
    )
    rules, onset, reported, facility_keys = _evaluate_rules(
        df, facility_to_district, district_names, valid_district_ids
    )

    output = _prepare_geography(df, facility_to_district, district_names)
    output["patient_id"] = output["patient_id"].map(lambda value: _hash_patient_id(value, salt))
    output["age_group"] = output["age_years"].apply(build_age_group)
    failed_mask = _combine_rule_failures(rules, df.index)
    quarantine_positions = np.flatnonzero(failed_mask.to_numpy(dtype=bool))
    quarantine_codes = np.full(len(quarantine_positions), "", dtype=object)
    for rule_id, passed in rules.items():
        if passed is None or not len(quarantine_positions):
            continue
        failed = ~passed.iloc[quarantine_positions].to_numpy(dtype=bool)
        previous = quarantine_codes[failed]
        quarantine_codes[failed] = [
            rule_id if not existing else f"{existing}|{rule_id}"
            for existing in previous
        ]
    reason_codes = np.full(len(df), "", dtype=object)
    reason_codes[quarantine_positions] = quarantine_codes
    output["dq_reason_codes"] = reason_codes
    primary_codes = np.full(len(df), "", dtype=object)
    primary_codes[quarantine_positions] = [
        value.split("|", 1)[0] for value in quarantine_codes
    ]
    output["dq_reason_code"] = primary_codes

    quarantine = output.loc[failed_mask].copy()
    clean = output.loc[~failed_mask].copy()

    clean_path = Path(clean_path)
    quarantine_path = Path(quarantine_path)
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    clean.to_parquet(clean_path, index=False)
    quarantine.to_csv(quarantine_path, index=False)

    report_path = Path(report_path) if report_path is not None else quarantine_path.parent / "dq_report.json"
    _write_dq_report(
        report_path,
        rows_in=len(df),
        rows_out=len(clean),
        rows_quarantined=len(quarantine),
        rules=rules,
    )

    print(f"DQ completed: clean={len(clean)} quarantined={len(quarantine)}")
    print(f"clean output: {clean_path}")
    print(f"quarantine output: {quarantine_path}")
    print(f"DQ report: {report_path} and {report_path.with_suffix('.csv')}")
    return clean, quarantine


def main():
    ap = argparse.ArgumentParser(description="Apply quality and privacy checks to encounter data.")
    ap.add_argument("--input", type=str, default=str(DEFAULT_INPUT), help="Directory containing encounter CSV files.")
    ap.add_argument("--clean", type=str, default=str(DEFAULT_CLEAN), help="Path for the cleaned record parquet file.")
    ap.add_argument("--quarantine", type=str, default=str(DEFAULT_QUARANTINE), help="Path for quarantined records CSV.")
    ap.add_argument("--report", type=str, default=str(DEFAULT_REPORT), help="Path for the per-load JSON DQ report.")
    args = ap.parse_args()

    apply_quality_and_privacy_checks(
        Path(args.input), Path(args.clean), Path(args.quarantine), Path(args.report)
    )


if __name__ == "__main__":
    main()
