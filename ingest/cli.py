"""Command-line entry point.

  python -m ingest.cli                       # all datasets, live CMS data
  python -m ingest.cli --dataset inpatient_drg
  python -m ingest.cli --source-dir tests/fixtures/phase1 --min-rows 1   # offline fixtures
"""

import argparse
import json
import logging
import sys

from ingest.config import load_datasets
from ingest.pipeline import run_dataset


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest CMS hospital datasets into the raw schema")
    p.add_argument("--dataset", default="all", help="dataset name(s), comma-separated, or 'all'")
    p.add_argument("--source-dir", help="load <dir>/<dataset>/<release>.csv instead of downloading")
    p.add_argument("--force", action="store_true", help="reload even if the same file was loaded before")
    p.add_argument("--min-rows", type=int, help="override the configured minimum row count")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    names = list(load_datasets()) if args.dataset == "all" else [d.strip() for d in args.dataset.split(",")]
    results, failures = [], []
    for name in names:  # keep going so one bad source doesn't hide problems in the others
        try:
            results.extend(run_dataset(name, source_dir=args.source_dir, force=args.force, min_rows=args.min_rows))
        except Exception as e:  # noqa: BLE001
            logging.getLogger("ingest").error("%s: ingestion failed: %s", name, e)
            failures.append({"dataset": name, "status": "failed", "error": f"{type(e).__name__}: {e}"})
    print(json.dumps(results + failures, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
