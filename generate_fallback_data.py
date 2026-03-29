"""
generate_fallback_data.py
-------------------------
Generates realistic synthetic US COVID-19 CSVs so the pipeline runs
offline / in environments where CDC APIs are unreachable.

Run this once:  python generate_fallback_data.py
"""

import numpy as np
import pandas as pd
from pathlib import Path

OUT = Path(__file__).parent / "data" / "fallback"
OUT.mkdir(parents=True, exist_ok=True)

RNG = np.random.default_rng(42)

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

STATE_POP = {
    "AL":4903185,"AK":731545,"AZ":7278717,"AR":3017804,"CA":39512223,
    "CO":5758736,"CT":3565287,"DE":973764,"FL":21477737,"GA":10617423,
    "HI":1415872,"ID":1787065,"IL":12671821,"IN":6732219,"IA":3155070,
    "KS":2913314,"KY":4467673,"LA":4648794,"ME":1344212,"MD":6045680,
    "MA":6892503,"MI":9986857,"MN":5639632,"MS":2976149,"MO":6137428,
    "MT":1068778,"NE":1934408,"NV":3080156,"NH":1359711,"NJ":8882190,
    "NM":2096829,"NY":19453561,"NC":10488084,"ND":762062,
    "OH":11689100,"OK":3956971,"OR":4217737,"PA":12801989,"RI":1059361,
    "SC":5148714,"SD":884659,"TN":6829174,"TX":28995881,"UT":3205958,
    "VT":623989,"VA":8535519,"WA":7614893,"WV":1792147,"WI":5822434,
    "WY":578759,"DC":705749,
}

DATES = pd.date_range("2021-01-01", "2023-06-30", freq="D")


def wave(dates, peak_day, width, height):
    """Gaussian-shaped epidemic wave."""
    x = np.array([(d - dates[0]).days for d in dates])
    return np.maximum(0, height * np.exp(-((x - peak_day) ** 2) / (2 * width**2)))


def make_cases():
    rows = []
    for state in STATES:
        pop = STATE_POP.get(state, 2_000_000)
        scale = pop / 1_000_000

        # three waves
        base = (
            wave(DATES, 80,  45, 500 * scale)   # delta
            + wave(DATES, 200, 30, 2000 * scale) # omicron
            + wave(DATES, 380, 50, 300 * scale)  # ba5
        )
        noise = RNG.normal(0, base * 0.1 + 1)
        new_cases  = np.maximum(0, base + noise).astype(int)
        new_deaths = np.maximum(0, new_cases * RNG.uniform(0.008, 0.018) + RNG.normal(0, 1)).astype(int)
        tot_cases  = np.cumsum(new_cases)
        tot_deaths = np.cumsum(new_deaths)

        for i, d in enumerate(DATES):
            rows.append({
                "submission_date": d.strftime("%Y-%m-%dT00:00:00.000"),
                "state":           state,
                "tot_cases":       int(tot_cases[i]),
                "new_case":        int(new_cases[i]),
                "tot_death":       int(tot_deaths[i]),
                "new_death":       int(new_deaths[i]),
            })

    df = pd.DataFrame(rows)
    path = OUT / "cases_fallback.csv"
    df.to_csv(path, index=False)
    print(f"✓ cases_fallback.csv  ({len(df):,} rows)  →  {path}")
    return df


def make_vax_state():
    rows = []
    for state in STATES:
        pop = STATE_POP.get(state, 2_000_000)
        # Sigmoid adoption curve starting mid-2021
        x = np.array([(d - DATES[0]).days for d in DATES])
        rate = 1 / (1 + np.exp(-(x - 120) / 30))
        cap_pct = RNG.uniform(0.55, 0.82)  # state max vax rate

        fully_vax  = (pop * cap_pct * rate).astype(int)
        boosters   = (fully_vax * 0.55 * np.clip((x - 200) / 200, 0, 1)).astype(int)
        doses      = fully_vax * 2 + boosters
        daily_doses = np.maximum(0, np.diff(doses, prepend=0))

        janssen = (doses * 0.08).astype(int)
        moderna = (doses * 0.35).astype(int)
        pfizer  = (doses * 0.57).astype(int)

        for i, d in enumerate(DATES):
            rows.append({
                "date":                  d.strftime("%Y-%m-%dT00:00:00.000"),
                "location":              state,
                "administered_cum":      int(doses[i]),
                "series_complete_cum":   int(fully_vax[i]),
                "booster_cum":           int(boosters[i]),
                "administered_janssen":  int(janssen[i]),
                "administered_moderna":  int(moderna[i]),
                "administered_pfizer":   int(pfizer[i]),
            })

    df = pd.DataFrame(rows)
    path = OUT / "vax_state_fallback.csv"
    df.to_csv(path, index=False)
    print(f"✓ vax_state_fallback.csv  ({len(df):,} rows)  →  {path}")
    return df


def make_vax_national():
    pop_us = 331_000_000
    x = np.array([(d - DATES[0]).days for d in DATES])
    rate = 1 / (1 + np.exp(-(x - 120) / 30))

    fully_vax  = (pop_us * 0.68 * rate).astype(int)
    boosters   = (fully_vax * 0.55 * np.clip((x - 200) / 200, 0, 1)).astype(int)
    doses      = fully_vax * 2 + boosters
    daily_doses = np.maximum(0, np.diff(doses, prepend=0))

    rows = []
    for i, d in enumerate(DATES):
        rows.append({
            "date":                d.strftime("%Y-%m-%dT00:00:00.000"),
            "administered_cum":    int(doses[i]),
            "series_complete_cum": int(fully_vax[i]),
            "booster_cum":         int(boosters[i]),
        })

    df = pd.DataFrame(rows)
    path = OUT / "vax_national_fallback.csv"
    df.to_csv(path, index=False)
    print(f"✓ vax_national_fallback.csv  ({len(df):,} rows)  →  {path}")
    return df


if __name__ == "__main__":
    print("Generating fallback CSVs …")
    make_cases()
    make_vax_state()
    make_vax_national()
    print("\nDone. Fallback data written to:", OUT)
