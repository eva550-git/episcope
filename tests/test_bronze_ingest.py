import hashlib
import json

from pipeline.bronze.ingest_bronze import classify, discover, sha256_of


def test_file_checksum_uses_sha256(tmp_path):
    source = tmp_path / "payload.csv"
    source.write_bytes(b"a,b\n1,2\n")

    assert sha256_of(source) == hashlib.sha256(source.read_bytes()).hexdigest()


def test_classifies_inputs_by_content_not_filename(tmp_path):
    encounter = tmp_path / "arbitrary-name.csv"
    encounter.write_text("encounter_id,report_date\nE1,2025-01-01\n", encoding="utf-8")
    stock = tmp_path / "snapshot.json"
    stock.write_text(json.dumps({"snapshot_date": "2025-01-01"}), encoding="utf-8")
    daily_counts = tmp_path / "counts.csv"
    daily_counts.write_text(
        "district_id,disease,date,cases\nBD1004,dengue,2025-01-01,3\n",
        encoding="utf-8",
    )

    assert classify(encounter) == "encounters"
    assert classify(stock) == "stock"
    assert classify(daily_counts) == "daily_counts"


def test_discovery_excludes_ground_truth_files(tmp_path):
    landing = tmp_path / "landing"
    truth = tmp_path / "ground_truth"
    landing.mkdir()
    truth.mkdir()
    accepted = landing / "drop.csv"
    excluded = truth / "outbreaks.csv"
    accepted.write_text("a,b\n1,2\n", encoding="utf-8")
    excluded.write_text("a,b\n1,2\n", encoding="utf-8")

    assert discover([tmp_path]) == [accepted]
