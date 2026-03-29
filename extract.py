"""
extract.py
----------
Pulls US COVID-19 data from:
  1. CDC Socrata REST APIs  (cases/deaths, vaccinations by state)
  2. A bundled fallback CSV (so the pipeline runs offline / in CI)
"""

import io
import logging
import requests
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── CDC Socrata endpoints ────────────────────────────────────────────────────
CDC_CASES_URL       = "https://data.cdc.gov/resource/9mfq-cb36.json"
CDC_VAX_STATE_URL   = "https://data.cdc.gov/resource/unsk-b7fc.json"
CDC_VAX_NATIONAL_URL= "https://data.cdc.gov/resource/rh2h-3yt2.json"

FALLBACK_CSV_DIR = Path(__file__).parent / "data" / "fallback"

# How many rows to pull per API request (Socrata max = 50 000)
API_LIMIT = 50_000


def _fetch_cdc_api(url: str, params: dict | None = None) -> pd.DataFrame:
    """Generic CDC Socrata fetch with pagination."""
    params = params or {}
    params.setdefault("$limit", API_LIMIT)
    params.setdefault("$offset", 0)

    frames = []
    while True:
        log.info("GET %s  offset=%s", url, params["$offset"])
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        frames.append(pd.DataFrame(batch))
        if len(batch) < API_LIMIT:
            break
        params["$offset"] += API_LIMIT

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def extract_cases() -> pd.DataFrame:
    """
    National COVID-19 cases & deaths by state/territory from CDC.
    Falls back to bundled CSV when the API is unreachable.
    """
    fallback = FALLBACK_CSV_DIR / "cases_fallback.csv"
    try:
        df = _fetch_cdc_api(
            CDC_CASES_URL,
            params={
                "$select": "submission_date,state,tot_cases,new_case,tot_death,new_death",
                "$order":  "submission_date DESC",
                "$limit":  API_LIMIT,
            },
        )
        log.info("Cases API → %d rows", len(df))
        return df
    except Exception as exc:
        log.warning("Cases API failed (%s). Loading fallback CSV.", exc)
        return pd.read_csv(fallback)


def extract_vaccinations_by_state() -> pd.DataFrame:
    """
    State-level vaccination data from CDC.
    Falls back to bundled CSV when the API is unreachable.
    """
    fallback = FALLBACK_CSV_DIR / "vax_state_fallback.csv"
    try:
        df = _fetch_cdc_api(
            CDC_VAX_STATE_URL,
            params={
                "$select": "date,location,administered_cum,series_complete_cum,"
                           "booster_cum,administered_janssen,administered_moderna,"
                           "administered_pfizer",
                "$order":  "date DESC",
                "$limit":  API_LIMIT,
            },
        )
        log.info("Vax-by-state API → %d rows", len(df))
        return df
    except Exception as exc:
        log.warning("Vax-by-state API failed (%s). Loading fallback CSV.", exc)
        return pd.read_csv(fallback)


def extract_vaccinations_national() -> pd.DataFrame:
    """
    National-level daily vaccination totals from CDC.
    Falls back to bundled CSV when the API is unreachable.
    """
    fallback = FALLBACK_CSV_DIR / "vax_national_fallback.csv"
    try:
        df = _fetch_cdc_api(
            CDC_VAX_NATIONAL_URL,
            params={
                "$select": "date,administered_cum,series_complete_cum,booster_cum",
                "$order":  "date DESC",
                "$limit":  API_LIMIT,
            },
        )
        log.info("Vax-national API → %d rows", len(df))
        return df
    except Exception as exc:
        log.warning("Vax-national API failed (%s). Loading fallback CSV.", exc)
        return pd.read_csv(fallback)


def extract_all() -> dict[str, pd.DataFrame]:
    """Entry point: returns a dict of raw DataFrames keyed by table name."""
    return {
        "raw_cases":           extract_cases(),
        "raw_vax_state":       extract_vaccinations_by_state(),
        "raw_vax_national":    extract_vaccinations_national(),
    }
