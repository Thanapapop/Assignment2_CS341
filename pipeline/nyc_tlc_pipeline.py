"""Reproducible NYC TLC trip-record pipeline.

The pipeline keeps source acquisition, inspection, curation, quality checks,
provenance, and reporting as explicit stages. It supports a full run and an
incremental run over any inclusive month range.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow.parquet as pq

SOURCE_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
TLC_YELLOW_DICTIONARY = "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"
TLC_HVFHVS_DICTIONARY = "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf"

SERVICE_CONFIG: dict[str, dict[str, str]] = {
    "yellow": {"label": "Yellow Taxi", "pickup_column": "tpep_pickup_datetime", "color": "#F2C230"},
    "green": {"label": "Green Taxi", "pickup_column": "lpep_pickup_datetime", "color": "#36A269"},
    "fhv": {"label": "For-Hire Vehicle (FHV)", "pickup_column": "pickup_datetime", "color": "#7D64B8"},
    "fhvhv": {"label": "High Volume FHV (HVFHV)", "pickup_column": "pickup_datetime", "color": "#153B5B"},
}


@dataclass(frozen=True)
class PipelineConfig:
    root: Path
    year: int
    start_month: int
    end_month: int
    download: bool
    force: bool

    @property
    def raw_dir(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def curated_dir(self) -> Path:
        return self.root / "data" / "curated"

    @property
    def reports_dir(self) -> Path:
        return self.root / "reports"

    @property
    def run_dir(self) -> Path:
        return self.root / "runs"

    @property
    def period_start(self) -> str:
        return f"{self.year:04d}-{self.start_month:02d}-01 00:00:00"

    @property
    def period_end(self) -> str:
        if self.end_month == 12:
            return f"{self.year + 1:04d}-01-01 00:00:00"
        return f"{self.year:04d}-{self.end_month + 1:02d}-01 00:00:00"

    @property
    def run_id(self) -> str:
        return f"{self.year}_{self.start_month:02d}_{self.end_month:02d}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def month_range(start: int, end: int) -> range:
    if not 1 <= start <= 12 or not 1 <= end <= 12 or start > end:
        raise ValueError("start_month and end_month must be between 1 and 12, with start <= end")
    return range(start, end + 1)


def file_name(service_key: str, year: int, month: int) -> str:
    return f"{service_key}_tripdata_{year:04d}-{month:02d}.parquet"


def parse_file_name(path: Path) -> tuple[str, int, int]:
    match = re.fullmatch(r"(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(\d{2})\.parquet", path.name)
    if not match:
        raise ValueError(f"Unexpected TLC raw filename: {path.name}")
    return match.group(1), int(match.group(2)), int(match.group(3))


def build_source_manifest(config: PipelineConfig) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for service_key in SERVICE_CONFIG:
        for month in month_range(config.start_month, config.end_month):
            name = file_name(service_key, config.year, month)
            records.append({
                "source_kind": "trip_record_parquet",
                "service_key": service_key,
                "service_type": SERVICE_CONFIG[service_key]["label"],
                "year": config.year,
                "month": month,
                "file_name": name,
                "source_url": f"{SOURCE_BASE}/{name}",
                "landing_page": TLC_PAGE,
                "dictionary_url": TLC_YELLOW_DICTIONARY if service_key == "yellow" else TLC_HVFHVS_DICTIONARY if service_key == "fhvhv" else TLC_PAGE,
            })
    return pd.DataFrame(records)


def acquire_sources(config: PipelineConfig, manifest: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    config.raw_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for row in manifest.to_dict("records"):
        target = config.raw_dir / row["file_name"]
        downloaded = False
        if not target.exists() and config.download:
            logger.info("Downloading %s", row["file_name"])
            urllib.request.urlretrieve(row["source_url"], target)
            downloaded = True
        if not target.exists():
            raise FileNotFoundError(f"Missing source file: {target}. Run with --download.")
        results.append({
            **row,
            "local_path": str(target.relative_to(config.root)),
            "downloaded_this_run": downloaded,
            "byte_size": target.stat().st_size,
            "sha256": sha256_file(target),
            "acquired_at_utc": datetime.now(timezone.utc).isoformat(),
        })
    return pd.DataFrame(results)


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def inspect_file(connection: duckdb.DuckDBPyConnection, config: PipelineConfig, source: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    path = config.root / source["local_path"]
    service_key, year, month = parse_file_name(path)
    service = SERVICE_CONFIG[service_key]
    parquet = pq.ParquetFile(path)
    fields = [field.name for field in parquet.schema_arrow]
    pickup = service["pickup_column"]
    if pickup not in fields:
        raise ValueError(f"Required pickup column {pickup} is absent from {path.name}")
    pickup_sql = quote_identifier(pickup)
    query = f"""
        SELECT
            COUNT(*) AS raw_rows,
            COALESCE(SUM(CASE WHEN {pickup_sql} IS NULL THEN 1 ELSE 0 END), 0) AS pickup_nulls,
            COALESCE(SUM(CASE WHEN {pickup_sql} >= TIMESTAMP '{config.period_start}' AND {pickup_sql} < TIMESTAMP '{config.period_end}' THEN 1 ELSE 0 END), 0) AS valid_trip_records,
            MIN({pickup_sql}) AS min_pickup_datetime,
            MAX({pickup_sql}) AS max_pickup_datetime
        FROM read_parquet(?)
    """
    raw_rows, pickup_nulls, valid, min_pickup, max_pickup = connection.execute(query, [str(path)]).fetchone()
    out_of_window = int(raw_rows) - int(pickup_nulls) - int(valid)
    usage = {
        "service_key": service_key,
        "service_type": service["label"],
        "year": year,
        "month": month,
        "source_file": path.name,
        "pickup_column": pickup,
        "raw_rows": int(raw_rows),
        "pickup_nulls": int(pickup_nulls),
        "out_of_window": out_of_window,
        "excluded_records": int(raw_rows) - int(valid),
        "valid_trip_records": int(valid),
        "min_pickup_datetime": str(min_pickup) if min_pickup else None,
        "max_pickup_datetime": str(max_pickup) if max_pickup else None,
    }
    inventory = {
        **source,
        "parquet_row_groups": parquet.metadata.num_row_groups,
        "parquet_rows_metadata": parquet.metadata.num_rows,
        "field_count": len(fields),
        "fields": " | ".join(fields),
        "schema_fingerprint": hashlib.sha256("|".join(fields).encode()).hexdigest(),
    }
    return inventory, usage


def run_quality_gates(inventory: pd.DataFrame, monthly: pd.DataFrame, expected_files: int) -> dict[str, Any]:
    checks = {
        "expected_file_count": {"passed": len(inventory) == expected_files, "observed": len(inventory), "expected": expected_files},
        "metadata_matches_raw_rows": {"passed": bool((inventory["parquet_rows_metadata"].astype(int).to_numpy() == monthly["raw_rows"].astype(int).to_numpy()).all())},
        "pickup_columns_present": {"passed": bool(monthly["pickup_column"].notna().all())},
        "non_negative_counts": {"passed": bool((monthly["valid_trip_records"] >= 0).all())},
        "valid_not_above_raw": {"passed": bool((monthly["valid_trip_records"] <= monthly["raw_rows"]).all())},
        "no_null_pickup_in_curated": {"passed": int(monthly["pickup_nulls"].sum()) == 0, "observed": int(monthly["pickup_nulls"].sum())},
    }
    failed = [name for name, check in checks.items() if not check["passed"]]
    return {"status": "PASS" if not failed else "FAIL", "checks": checks, "failed_checks": failed}


def curate_and_report(config: PipelineConfig, inventory: pd.DataFrame, monthly: pd.DataFrame, quality: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    config.curated_dir.mkdir(parents=True, exist_ok=True)
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    if quality["status"] != "PASS":
        raise RuntimeError(f"Quality gates failed: {quality['failed_checks']}")
    monthly = monthly.sort_values(["service_key", "year", "month"]).reset_index(drop=True)
    half_year = (
        monthly.groupby(["service_key", "service_type"], as_index=False)
        .agg(raw_rows=("raw_rows", "sum"), pickup_nulls=("pickup_nulls", "sum"), out_of_window=("out_of_window", "sum"), excluded_records=("excluded_records", "sum"), valid_trip_records=("valid_trip_records", "sum"))
        .sort_values("valid_trip_records", ascending=False)
        .reset_index(drop=True)
    )
    total_valid = int(half_year["valid_trip_records"].sum())
    half_year["share_of_valid_trips_pct"] = (half_year["valid_trip_records"] / total_valid * 100).round(4)
    half_year["rank"] = range(1, len(half_year) + 1)
    taxicab = half_year[half_year.service_key.isin(["yellow", "green"])].copy().sort_values("valid_trip_records", ascending=False).reset_index(drop=True)
    taxicab["share_of_yellow_green_pct"] = (taxicab["valid_trip_records"] / taxicab["valid_trip_records"].sum() * 100).round(4)
    taxicab["rank"] = range(1, len(taxicab) + 1)

    inventory.to_csv(config.curated_dir / "source_inventory.csv", index=False)
    monthly.to_csv(config.curated_dir / "monthly_usage.csv", index=False)
    half_year.to_csv(config.curated_dir / "half_year_usage.csv", index=False)
    taxicab.to_csv(config.curated_dir / "yellow_green_sensitivity.csv", index=False)
    pd.DataFrame([{"service_key": key, "service_type": value["label"], "pickup_column": value["pickup_column"]} for key, value in SERVICE_CONFIG.items()]).to_csv(config.curated_dir / "service_schema_map.csv", index=False)

    winner = half_year.iloc[0]
    metrics = {
        "question": "Which taxi type was the most used in the first half of 2024?",
        "period_start": config.period_start,
        "period_end_exclusive": config.period_end,
        "definition": "Count of trip-record rows whose service-specific pickup timestamp is non-null and inside the requested period.",
        "source_page": TLC_PAGE,
        "source_files": int(len(inventory)),
        "raw_rows": int(monthly.raw_rows.sum()),
        "valid_trip_records_total": total_valid,
        "winner": {"service_key": winner.service_key, "service_type": winner.service_type, "valid_trip_records": int(winner.valid_trip_records), "share_pct": float(winner.share_of_valid_trips_pct)},
        "strict_taxicab_winner": {"service_type": taxicab.iloc[0].service_type, "valid_trip_records": int(taxicab.iloc[0].valid_trip_records), "share_pct": float(taxicab.iloc[0].share_of_yellow_green_pct)},
        "quality": quality,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    with (config.reports_dir / "analysis_metrics.json").open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, ensure_ascii=False)
    logger.info("Winner: %s (%s records; %.4f%%)", winner.service_type, winner.valid_trip_records, winner.share_of_valid_trips_pct)
    return metrics


def write_run_artifacts(config: PipelineConfig, manifest: pd.DataFrame, metrics: dict[str, Any], started_at: str, status: str) -> None:
    config.run_dir.mkdir(parents=True, exist_ok=True)
    run_path = config.run_dir / f"run_{config.run_id}.json"
    payload = {
        "run_id": config.run_id,
        "status": status,
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "full" if config.force else "incremental-compatible",
        "parameters": {"year": config.year, "start_month": config.start_month, "end_month": config.end_month, "download": config.download, "force": config.force},
        "source_manifest_rows": int(len(manifest)),
        "metrics_file": str((config.reports_dir / "analysis_metrics.json").relative_to(config.root)) if metrics else None,
        "winner": metrics.get("winner") if metrics else None,
    }
    run_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (config.run_dir / "latest.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def configure_logging(root: Path, run_id: str) -> tuple[logging.Logger, Path]:
    log_dir = root / "runs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"pipeline_{run_id}.log"
    logger = logging.getLogger("nyc_tlc_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)sZ | %(levelname)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger, log_path


def run(config: PipelineConfig) -> dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat()
    logger, log_path = configure_logging(config.root, config.run_id)
    manifest = build_source_manifest(config)
    manifest_path = config.root / "config" / "source_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(manifest_path, index=False)
    logger.info("Pipeline started: %s", config.run_id)
    logger.info("Source manifest: %s rows", len(manifest))
    metrics: dict[str, Any] = {}
    try:
        acquired = acquire_sources(config, manifest, logger)
        connection = duckdb.connect()
        inventories: list[dict[str, Any]] = []
        usages: list[dict[str, Any]] = []
        for source in acquired.to_dict("records"):
            logger.info("Inspecting %s", source["file_name"])
            inventory, usage = inspect_file(connection, config, source)
            inventories.append(inventory)
            usages.append(usage)
        connection.close()
        inventory_df = pd.DataFrame(inventories).sort_values(["service_key", "year", "month"]).reset_index(drop=True)
        monthly_df = pd.DataFrame(usages).sort_values(["service_key", "year", "month"]).reset_index(drop=True)
        quality = run_quality_gates(inventory_df, monthly_df, len(manifest))
        (config.reports_dir).mkdir(parents=True, exist_ok=True)
        (config.reports_dir / "quality_report.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")
        logger.info("Quality gates: %s", quality["status"])
        metrics = curate_and_report(config, inventory_df, monthly_df, quality, logger)
        write_run_artifacts(config, manifest, metrics, started, "SUCCESS")
        logger.info("Pipeline completed successfully; log=%s", log_path)
        return metrics
    except Exception:
        write_run_artifacts(config, manifest, metrics, started, "FAILED")
        logger.exception("Pipeline failed")
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the reproducible NYC TLC curated-data pipeline.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=6)
    parser.add_argument("--download", action="store_true", help="Download missing TLC Parquet files from the official source.")
    parser.add_argument("--force", action="store_true", help="Record this run as a full rebuild; outputs are always rebuilt.")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(PipelineConfig(root=args.root.resolve(), year=args.year, start_month=args.start_month, end_month=args.end_month, download=args.download, force=args.force))
