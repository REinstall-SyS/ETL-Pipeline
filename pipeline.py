"""
pipeline.py
-----------
Orchestrates the full ETL pipeline:
  1. Extract  – pull data from CDC APIs / fallback CSVs
  2. Transform – clean, normalise, validate with Pandas
  3. Load      – persist to normalised SQLite database

Run:
    python pipeline.py              # full pipeline
    python pipeline.py --db custom.db
"""

import argparse
import logging
import time
from pathlib import Path

from extract   import extract_all
from transform import transform_all
from load      import load_all, DEFAULT_DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def run_pipeline(db_path: Path = DEFAULT_DB) -> None:
    start = time.perf_counter()
    log.info("═" * 55)
    log.info("  US COVID-19 ETL Pipeline  –  starting")
    log.info("═" * 55)

    # ── EXTRACT ──────────────────────────────────────────
    log.info("[ 1/3 ] EXTRACT")
    raw = extract_all()
    for name, df in raw.items():
        log.info("  %-25s → %d rows", name, len(df))

    # ── TRANSFORM ────────────────────────────────────────
    log.info("[ 2/3 ] TRANSFORM")
    clean = transform_all(raw)
    for name, df in clean.items():
        log.info("  %-25s → %d rows, %d cols", name, *df.shape)

    # ── LOAD ─────────────────────────────────────────────
    log.info("[ 3/3 ] LOAD  →  %s", db_path)
    conn = load_all(clean, db_path)
    conn.close()

    elapsed = time.perf_counter() - start
    log.info("═" * 55)
    log.info("  Pipeline complete in %.1f s", elapsed)
    log.info("═" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the COVID-19 ETL pipeline.")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to output SQLite database (default: data/covid.db)",
    )
    args = parser.parse_args()
    run_pipeline(db_path=args.db)
