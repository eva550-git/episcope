"""Silver layer orchestration."""

from pipeline.quality.quality_rules import apply_quality_and_privacy_checks


if __name__ == "__main__":
    apply_quality_and_privacy_checks()
