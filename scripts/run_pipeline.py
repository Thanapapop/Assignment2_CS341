#!/usr/bin/env python3
"""Small command-line entry point for the reproducible pipeline."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.nyc_tlc_pipeline import PipelineConfig, build_parser, run  # noqa: E402


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(PipelineConfig(root=args.root.resolve(), year=args.year, start_month=args.start_month, end_month=args.end_month, download=args.download, force=args.force))
