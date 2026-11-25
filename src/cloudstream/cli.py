"""Command line entry point for a single ingest date."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path

from cloudstream.config import Settings
from cloudstream.landing import land_bronze
from cloudstream.pipeline import run_day, run_gold, run_quality, run_silver
from cloudstream.quality import QualityError
from cloudstream.schemas import SchemaError
from cloudstream.warehouse import load_star_schema


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CloudStream daily lakehouse load.")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("land", "Copy one day's CSV and JSON into the bronze partition"),
        ("silver", "Enforce schema, clean, and deduplicate that partition"),
        ("gold", "Update the star schema in the lake"),
        ("load", "MERGE the partition into the warehouse"),
        ("quality", "Run row-count and uniqueness checks"),
        ("run", "Land, curate, check, and load one date"),
    ):
        command = sub.add_parser(name, help=help_text)
        command.add_argument("--date", required=True, help="Ingest date, YYYY-MM-DD")
        if name in {"land", "run"}:
            command.add_argument("--source", help="Directory that holds daily extract folders")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    source = getattr(args, "source", None)
    if source:
        settings = replace(settings, source_dir=Path(source))
    try:
        if args.command == "land":
            written = land_bronze(settings, args.date)
            print(f"Landed {len(written)} files for {args.date}")
        elif args.command == "silver":
            summary = run_silver(settings, args.date)
            print(summary)
        elif args.command == "gold":
            run_gold(settings, args.date)
            print(f"Gold tables updated for {args.date}")
        elif args.command == "load":
            load_star_schema(settings, args.date)
            print(f"Warehouse load committed for {args.date}")
        elif args.command == "quality":
            print(run_quality(settings, args.date))
        else:
            summary = run_day(settings, args.date)
            print(
                f"Loaded {args.date}: "
                f"orders={summary['silver']['orders']['silver']} "
                f"customers={summary['silver']['customers']['silver']} "
                f"products={summary['silver']['products']['silver']}"
            )
    except (QualityError, SchemaError, FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
