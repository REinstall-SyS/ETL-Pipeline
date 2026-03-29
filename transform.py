"""
transform.py
------------
Pandas transformations applied to raw CDC extracts:
  - Schema normalisation   (column rename, type casting)
  - Missing-value strategy (impute, forward-fill, drop)
  - Data validation        (range checks, duplicate removal, referential checks)
  - Feature engineering    (7-day rolling averages, per-100k rates)
"""

import logging
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# US states + territories recognised by CDC (used for referential validation)
VALID_LOCATIONS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NYC","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY","DC","PR","VI","GU","MP",
    "AS","FSM","RMI","PW","MH","US",
}

# State population estimates (2020 Census approximations) for per-100k rates
STATE_POPULATION = {
    "AL":4903185,"AK":731545,"AZ":7278717,"AR":3017804,"CA":39512223,
    "CO":5758736,"CT":3565287,"DE":973764,"FL":21477737,"GA":10617423,
    "HI":1415872,"ID":1787065,"IL":12671821,"IN":6732219,"IA":3155070,
    "KS":2913314,"KY":4467673,"LA":4648794,"ME":1344212,"MD":6045680,
    "MA":6892503,"MI":9986857,"MN":5639632,"MS":2976149,"MO":6137428,
    "MT":1068778,"NE":1934408,"NV":3080156,"NH":1359711,"NJ":8882190,
    "NM":2096829,"NY":19453561,"NYC":8336817,"NC":10488084,"ND":762062,
    "OH":11689100,"OK":3956971,"OR":4217737,"PA":12801989,"RI":1059361,
    "SC":5148714,"SD":884659,"TN":6829174,"TX":28995881,"UT":3205958,
    "VT":623989,"VA":8535519,"WA":7614893,"WV":1792147,"WI":5822434,
    "WY":578759,"DC":705749,"PR":3193694,
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _to_numeric_safe(series: pd.Series) -> pd.Series:
    """Coerce a column to float, turning non-numeric values into NaN."""
    return pd.to_numeric(series, errors="coerce")


def _parse_date(series: pd.Series) -> pd.Series:
    """Parse ISO-like date strings; coerce unparseable values to NaT."""
    return pd.to_datetime(series, errors="coerce", utc=False).dt.normalize()


def _validate_non_negative(df: pd.DataFrame, cols: list[str], name: str) -> None:
    for col in cols:
        if col not in df.columns:
            continue
        n_neg = (df[col] < 0).sum()
        if n_neg:
            log.warning("[%s] %d negative values in '%s' → set to NaN", name, n_neg, col)
            df.loc[df[col] < 0, col] = np.nan


def _log_nulls(df: pd.DataFrame, name: str) -> None:
    null_pct = (df.isnull().sum() / len(df) * 100).round(1)
    null_pct = null_pct[null_pct > 0]
    if not null_pct.empty:
        log.info("[%s] Null %% per column:\n%s", name, null_pct.to_string())


# ── cases transform ───────────────────────────────────────────────────────────

def transform_cases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input : raw_cases  (from extract.extract_cases)
    Output: clean cases table ready for SQLite load
    """
    log.info("Transforming cases (%d rows) …", len(df))

    # ── 1. Schema normalisation
    rename_map = {
        "submission_date": "report_date",
        "state":           "state_code",
        "tot_cases":       "total_cases",
        "new_case":        "new_cases",
        "tot_death":       "total_deaths",
        "new_death":       "new_deaths",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # ── 2. Type casting
    df["report_date"] = _parse_date(df["report_date"])
    for col in ["total_cases", "new_cases", "total_deaths", "new_deaths"]:
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])

    # ── 3. Drop rows with unusable date or state
    before = len(df)
    df = df.dropna(subset=["report_date", "state_code"])
    log.info("[cases] Dropped %d rows with null date/state", before - len(df))

    # ── 4. Referential validation: keep only recognised location codes
    invalid_states = ~df["state_code"].isin(VALID_LOCATIONS)
    if invalid_states.sum():
        log.warning("[cases] Dropping %d rows with unknown state codes", invalid_states.sum())
        df = df[~invalid_states]

    # ── 5. Range validation
    _validate_non_negative(df, ["total_cases", "new_cases", "total_deaths", "new_deaths"], "cases")

    # ── 6. Impute new_cases / new_deaths from cumulative if missing
    df = df.sort_values(["state_code", "report_date"])
    for cum_col, delta_col in [("total_cases", "new_cases"), ("total_deaths", "new_deaths")]:
        if cum_col in df.columns and delta_col in df.columns:
            mask = df[delta_col].isna() & df[cum_col].notna()
            df.loc[mask, delta_col] = (
                df.groupby("state_code")[cum_col]
                  .diff()
                  .loc[mask]
                  .clip(lower=0)
            )

    # ── 7. Forward-fill remaining nulls within each state (max 3 days)
    df[["total_cases", "total_deaths"]] = (
        df.groupby("state_code")[["total_cases", "total_deaths"]]
          .transform(lambda s: s.ffill(limit=3))
    )

    # ── 8. Deduplicate (keep last submission per state × date)
    before = len(df)
    df = df.drop_duplicates(subset=["report_date", "state_code"], keep="last")
    log.info("[cases] Removed %d duplicate rows", before - len(df))

    # ── 9. Feature engineering: 7-day rolling average
    for col in ["new_cases", "new_deaths"]:
        if col in df.columns:
            df[f"{col}_7d_avg"] = (
                df.groupby("state_code")[col]
                  .transform(lambda s: s.rolling(7, min_periods=1).mean().round(1))
            )

    # ── 10. Per-100k rates
    df["population"] = df["state_code"].map(STATE_POPULATION)
    for col in ["total_cases", "total_deaths"]:
        if col in df.columns:
            df[f"{col}_per_100k"] = (df[col] / df["population"] * 100_000).round(2)

    _log_nulls(df, "cases")
    log.info("Cases transform complete → %d rows, %d columns", *df.shape)
    return df.reset_index(drop=True)


# ── vaccinations by state transform ──────────────────────────────────────────

def transform_vax_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input : raw_vax_state  (from extract.extract_vaccinations_by_state)
    Output: clean state-level vaccination table
    """
    log.info("Transforming vax-by-state (%d rows) …", len(df))

    rename_map = {
        "date":                  "report_date",
        "location":              "state_code",
        "administered_cum":      "doses_administered",
        "series_complete_cum":   "fully_vaccinated",
        "booster_cum":           "boosters",
        "administered_janssen":  "doses_janssen",
        "administered_moderna":  "doses_moderna",
        "administered_pfizer":   "doses_pfizer",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df["report_date"] = _parse_date(df["report_date"])
    numeric_cols = [
        "doses_administered", "fully_vaccinated", "boosters",
        "doses_janssen", "doses_moderna", "doses_pfizer",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])

    df = df.dropna(subset=["report_date", "state_code"])

    invalid = ~df["state_code"].isin(VALID_LOCATIONS)
    if invalid.sum():
        log.warning("[vax_state] Dropping %d unknown state rows", invalid.sum())
        df = df[~invalid]

    _validate_non_negative(df, numeric_cols, "vax_state")

    df = df.sort_values(["state_code", "report_date"])
    df[numeric_cols] = (
        df.groupby("state_code")[numeric_cols]
          .transform(lambda s: s.ffill(limit=7))
    )

    before = len(df)
    df = df.drop_duplicates(subset=["report_date", "state_code"], keep="last")
    log.info("[vax_state] Removed %d duplicate rows", before - len(df))

    # Percentage vaccinated
    df["population"] = df["state_code"].map(STATE_POPULATION)
    if "fully_vaccinated" in df.columns:
        df["pct_fully_vaccinated"] = (
            (df["fully_vaccinated"] / df["population"] * 100).round(2).clip(upper=100)
        )

    _log_nulls(df, "vax_state")
    log.info("Vax-state transform complete → %d rows, %d columns", *df.shape)
    return df.reset_index(drop=True)


# ── vaccinations national transform ──────────────────────────────────────────

def transform_vax_national(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input : raw_vax_national  (from extract.extract_vaccinations_national)
    Output: clean national-level vaccination time series
    """
    log.info("Transforming vax-national (%d rows) …", len(df))

    rename_map = {
        "date":                "report_date",
        "administered_cum":    "doses_administered",
        "series_complete_cum": "fully_vaccinated",
        "booster_cum":         "boosters",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df["report_date"] = _parse_date(df["report_date"])
    numeric_cols = ["doses_administered", "fully_vaccinated", "boosters"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = _to_numeric_safe(df[col])

    df = df.dropna(subset=["report_date"])
    _validate_non_negative(df, numeric_cols, "vax_national")

    df = df.sort_values("report_date")
    df[numeric_cols] = df[numeric_cols].ffill(limit=7)

    before = len(df)
    df = df.drop_duplicates(subset=["report_date"], keep="last")
    log.info("[vax_national] Removed %d duplicate rows", before - len(df))

    # Daily doses administered (derived from cumulative)
    if "doses_administered" in df.columns:
        df["daily_doses"] = df["doses_administered"].diff().clip(lower=0)
        df["daily_doses_7d_avg"] = df["daily_doses"].rolling(7, min_periods=1).mean().round(0)

    _log_nulls(df, "vax_national")
    log.info("Vax-national transform complete → %d rows, %d columns", *df.shape)
    return df.reset_index(drop=True)


# ── public entry point ────────────────────────────────────────────────────────

def transform_all(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Applies all transformations.
    Expects keys: raw_cases, raw_vax_state, raw_vax_national
    Returns keys: cases, vax_state, vax_national
    """
    return {
        "cases":        transform_cases(raw["raw_cases"]),
        "vax_state":    transform_vax_state(raw["raw_vax_state"]),
        "vax_national": transform_vax_national(raw["raw_vax_national"]),
    }
