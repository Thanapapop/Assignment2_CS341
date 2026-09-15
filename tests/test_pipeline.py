from pathlib import Path

import pandas as pd

from pipeline.nyc_tlc_pipeline import (
    PipelineConfig,
    SERVICE_CONFIG,
    build_source_manifest,
    parse_file_name,
    run_quality_gates,
)


def test_source_manifest_has_all_service_month_combinations(tmp_path: Path):
    config = PipelineConfig(root=tmp_path, year=2024, start_month=1, end_month=6, download=False, force=False)
    manifest = build_source_manifest(config)
    assert len(manifest) == 24
    assert set(manifest["service_key"]) == set(SERVICE_CONFIG)
    assert set(manifest["month"]) == set(range(1, 7))
    assert manifest["source_url"].str.endswith(".parquet").all()


def test_parse_filename_supports_updates_and_rejects_bad_shape():
    assert parse_file_name(Path("yellow_tripdata_2024-06.parquet")) == ("yellow", 2024, 6)
    assert parse_file_name(Path("yellow_tripdata_2025-01.parquet")) == ("yellow", 2025, 1)
    try:
        parse_file_name(Path("unknown_tripdata_2024-06.csv"))
    except ValueError:
        pass
    else:
        raise AssertionError("Unexpected source filename should fail parsing")


def test_quality_gates_pass_for_consistent_curated_inputs():
    inventory = pd.DataFrame({"parquet_rows_metadata": [10, 20], "pickup_column": ["a", "b"]})
    monthly = pd.DataFrame({
        "raw_rows": [10, 20],
        "pickup_nulls": [0, 0],
        "valid_trip_records": [10, 19],
        "pickup_column": ["a", "b"],
    })
    quality = run_quality_gates(inventory, monthly, expected_files=2)
    assert quality["status"] == "PASS"
    assert quality["failed_checks"] == []


def test_quality_gates_fail_when_valid_exceeds_raw():
    inventory = pd.DataFrame({"parquet_rows_metadata": [10], "pickup_column": ["a"]})
    monthly = pd.DataFrame({
        "raw_rows": [10],
        "pickup_nulls": [0],
        "valid_trip_records": [11],
        "pickup_column": ["a"],
    })
    quality = run_quality_gates(inventory, monthly, expected_files=1)
    assert quality["status"] == "FAIL"
    assert "valid_not_above_raw" in quality["failed_checks"]
