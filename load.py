"""
load.py
-------
Designs and populates a normalized SQLite database.
Schema (3NF):
  dim_state        – state dimension table
  fact_cases       – daily cases/deaths by state
  fact_vax_state   – daily vaccination counts by state
  fact_vax_national– national daily vaccination totals

Indexing strategy:
  • Covering indexes on (state_code, report_date) for time-series filters
  • Index on report_date alone for national range scans
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent / "data" / "covid.db"


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """
-- ── dimension table ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_state (
    state_code   TEXT PRIMARY KEY,
    population   INTEGER
);

-- ── fact: daily cases / deaths ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_cases (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date             DATE    NOT NULL,
    state_code              TEXT    NOT NULL REFERENCES dim_state(state_code),
    total_cases             REAL,
    new_cases               REAL,
    total_deaths            REAL,
    new_deaths              REAL,
    new_cases_7d_avg        REAL,
    new_deaths_7d_avg       REAL,
    total_cases_per_100k    REAL,
    total_deaths_per_100k   REAL,
    UNIQUE (report_date, state_code)
);

CREATE INDEX IF NOT EXISTS idx_cases_state_date
    ON fact_cases (state_code, report_date);

CREATE INDEX IF NOT EXISTS idx_cases_date
    ON fact_cases (report_date);

-- ── fact: state-level vaccinations ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_vax_state (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date          DATE    NOT NULL,
    state_code           TEXT    NOT NULL REFERENCES dim_state(state_code),
    doses_administered   REAL,
    fully_vaccinated     REAL,
    boosters             REAL,
    doses_janssen        REAL,
    doses_moderna        REAL,
    doses_pfizer         REAL,
    pct_fully_vaccinated REAL,
    UNIQUE (report_date, state_code)
);

CREATE INDEX IF NOT EXISTS idx_vax_state_state_date
    ON fact_vax_state (state_code, report_date);

CREATE INDEX IF NOT EXISTS idx_vax_state_date
    ON fact_vax_state (report_date);

-- ── fact: national vaccinations ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_vax_national (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date          DATE    NOT NULL UNIQUE,
    doses_administered   REAL,
    fully_vaccinated     REAL,
    boosters             REAL,
    daily_doses          REAL,
    daily_doses_7d_avg   REAL
);

CREATE INDEX IF NOT EXISTS idx_vax_national_date
    ON fact_vax_national (report_date);
"""


# ── helpers ──────────────────────────────────────────────────────────────────

def get_connection(db_path: Path = DEFAULT_DB) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")   # better write concurrency
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;") # safer than OFF, faster than FULL
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()
    log.info("Schema initialised / verified.")


def _upsert_df(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table: str,
    conflict_cols: list[str],
) -> int:
    """
    Bulk-upsert a DataFrame into `table`.
    Uses INSERT OR REPLACE so re-running the pipeline is idempotent.
    Returns the number of rows written.
    """
    if df.empty:
        log.warning("No data to load into %s.", table)
        return 0

    # Only keep columns that exist in the target table
    cur = conn.execute(f"PRAGMA table_info({table})")
    db_cols = {row[1] for row in cur.fetchall()} - {"id"}
    df_cols = [c for c in df.columns if c in db_cols]
    df = df[df_cols].copy()

    # Convert dates to ISO strings for SQLite TEXT storage
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    placeholders = ", ".join(["?"] * len(df_cols))
    col_names    = ", ".join(df_cols)
    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    conn.executemany(sql, rows)
    conn.commit()
    log.info("Loaded %d rows into %s.", len(rows), table)
    return len(rows)


# ── dimension loaders ─────────────────────────────────────────────────────────

def load_dim_state(conn: sqlite3.Connection, cases_df: pd.DataFrame) -> None:
    """Populate dim_state from state codes and populations found in cases data."""
    dim = (
        cases_df[["state_code", "population"]]
        .dropna(subset=["state_code"])
        .drop_duplicates(subset=["state_code"])
    )
    _upsert_df(conn, dim, "dim_state", ["state_code"])


# ── fact loaders ──────────────────────────────────────────────────────────────

def load_fact_cases(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    return _upsert_df(conn, df, "fact_cases", ["report_date", "state_code"])


def load_fact_vax_state(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    return _upsert_df(conn, df, "fact_vax_state", ["report_date", "state_code"])


def load_fact_vax_national(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    return _upsert_df(conn, df, "fact_vax_national", ["report_date"])


# ── analytics queries (used by dashboard) ────────────────────────────────────

QUERIES = {
    # National daily new cases + 7-day rolling avg
    "national_cases_trend": """
        SELECT
            report_date,
            SUM(new_cases)         AS new_cases,
            AVG(new_cases_7d_avg)  AS new_cases_7d_avg
        FROM fact_cases
        GROUP BY report_date
        ORDER BY report_date;
    """,

    # Top-N states by total cases
    "top_states_cases": """
        SELECT
            state_code,
            MAX(total_cases)  AS total_cases,
            MAX(total_deaths) AS total_deaths
        FROM fact_cases
        GROUP BY state_code
        ORDER BY total_cases DESC
        LIMIT 15;
    """,

    # National vaccination progress over time
    "national_vax_trend": """
        SELECT
            report_date,
            doses_administered,
            fully_vaccinated,
            boosters,
            daily_doses_7d_avg
        FROM fact_vax_national
        ORDER BY report_date;
    """,

    # Latest vaccination % by state
    "latest_vax_by_state": """
        SELECT
            v.state_code,
            v.pct_fully_vaccinated,
            v.fully_vaccinated,
            s.population
        FROM fact_vax_state v
        JOIN dim_state s ON s.state_code = v.state_code
        WHERE v.report_date = (
            SELECT MAX(report_date) FROM fact_vax_state
        )
        ORDER BY pct_fully_vaccinated DESC;
    """,

    # Cases vs vaccinations correlation by state (latest snapshot)
    "state_cases_vs_vax": """
        SELECT
            c.state_code,
            c.total_cases_per_100k,
            c.total_deaths_per_100k,
            v.pct_fully_vaccinated
        FROM (
            SELECT state_code,
                   MAX(total_cases_per_100k)  AS total_cases_per_100k,
                   MAX(total_deaths_per_100k) AS total_deaths_per_100k
            FROM fact_cases
            GROUP BY state_code
        ) c
        LEFT JOIN (
            SELECT state_code, pct_fully_vaccinated
            FROM fact_vax_state
            WHERE report_date = (SELECT MAX(report_date) FROM fact_vax_state)
        ) v ON v.state_code = c.state_code
        WHERE c.state_code != 'US'
        ORDER BY c.total_cases_per_100k DESC;
    """,
}


def run_query(conn: sqlite3.Connection, query_name: str) -> pd.DataFrame:
    """Execute a named analytics query and return a DataFrame."""
    sql = QUERIES.get(query_name)
    if sql is None:
        raise ValueError(f"Unknown query: {query_name}")
    return pd.read_sql_query(sql, conn, parse_dates=["report_date"])


# ── public entry point ────────────────────────────────────────────────────────

def load_all(
    clean: dict[str, pd.DataFrame],
    db_path: Path = DEFAULT_DB,
) -> sqlite3.Connection:
    """
    Initialise DB, load dimensions then facts.
    Returns an open connection (caller is responsible for closing).
    """
    conn = get_connection(db_path)
    init_schema(conn)

    load_dim_state(conn, clean["cases"])
    load_fact_cases(conn, clean["cases"])
    load_fact_vax_state(conn, clean["vax_state"])
    load_fact_vax_national(conn, clean["vax_national"])

    log.info("All data loaded into %s", db_path)
    return conn
