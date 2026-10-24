import argparse

import pytest

from pipeline.experiments.scale_benchmarks import _directory_bytes, parse_scale_factors
from pipeline.experiments.spark_optimisations import _summary


def test_scale_factor_parser_accepts_required_factors():
    assert parse_scale_factors("0.25,0.5,1") == [0.25, 0.5, 1.0]


@pytest.mark.parametrize("value", ["", "0.25,0.25", "0,-1", "invalid"])
def test_scale_factor_parser_rejects_invalid_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_scale_factors(value)


def test_directory_size_counts_only_files(tmp_path):
    folder = tmp_path / "output"
    folder.mkdir()
    (folder / "one.bin").write_bytes(b"123")
    (folder / "two.bin").write_bytes(b"4567")

    assert _directory_bytes(folder) == 7


def test_benchmark_summary_reports_median_speedup():
    assert _summary([1.0, 2.0, 3.0], [0.5, 1.0, 1.5]) == {
        "baseline_median_seconds": 2.0,
        "optimized_median_seconds": 1.0,
        "speedup": 2.0,
    }
