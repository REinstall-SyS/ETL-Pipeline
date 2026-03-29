# US COVID-19 End-to-End Data Engineering Pipeline

A production-grade ETL pipeline that extracts US COVID-19 data from CDC APIs,
transforms and validates it with Pandas, persists it in a normalised SQLite
database, and visualises national vaccination trends in a Streamlit BI dashboard.

---

## Project Structure

```
covid_pipeline/
├── extract.py                  # CDC API ingestion + CSV fallback
├── transform.py                # Pandas normalisation, validation, feature engineering
├── load.py                     # Normalised SQLite schema + analytics queries
├── pipeline.py                 # ETL orchestrator (entry point)
├── dashboard.py                # Streamlit BI dashboard
├── generate_fallback_data.py   # One-time script to seed offline CSV data
├── requirements.txt
└── data/
    ├── fallback/               # Offline CSV seeds (auto-generated)
    └── covid.db                # SQLite database (created by pipeline)
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. (Optional) Generate offline fallback data
If you want the pipeline to work without internet access:
```bash
python generate_fallback_data.py
```
This creates realistic synthetic CSVs in `data/fallback/`. The pipeline falls
back to these automatically when CDC APIs are unreachable.

### 3. Run the ETL pipeline
```bash
python pipeline.py
```
Output: `data/covid.db`

### 4. Launch the dashboard
```bash
streamlit run dashboard.py
```
Opens at `http://localhost:8501`

---

## Architecture

```
CDC APIs / Fallback CSVs
        │
        ▼
  ┌─────────────┐
  │  extract.py │  ← requests + pagination
  └──────┬──────┘
         │  raw DataFrames
         ▼
  ┌───────────────┐
  │ transform.py  │  ← schema normalisation
  │               │    missing-value imputation
  │               │    range validation
  │               │    7-day rolling avgs
  │               │    per-100k rates
  └──────┬────────┘
         │  clean DataFrames
         ▼
  ┌──────────────┐
  │   load.py    │  ← normalised SQLite (3NF)
  │              │    dim_state
  │              │    fact_cases
  │              │    fact_vax_state
  │              │    fact_vax_national
  │              │    indexed for low-latency queries
  └──────┬───────┘
         │  SQLite
         ▼
  ┌───────────────┐
  │ dashboard.py  │  ← Streamlit + Plotly
  │               │    National trend charts
  │               │    Vaccination progress
  │               │    State comparison
  │               │    State drill-down
  └───────────────┘
```

---

## Data Sources

| Dataset | CDC Endpoint |
|---|---|
| Cases & Deaths by State | `data.cdc.gov/resource/9mfq-cb36.json` |
| Vaccinations by State | `data.cdc.gov/resource/unsk-b7fc.json` |
| National Vaccinations | `data.cdc.gov/resource/rh2h-3yt2.json` |

---

## Key Engineering Decisions

### Extract
- Paginated Socrata API calls (50 k rows/page) handle full dataset sizes
- Automatic fallback to bundled CSVs on network failure – pipeline never crashes

### Transform
- All raw columns renamed to snake_case for consistency
- `pd.to_numeric(errors='coerce')` + `pd.to_datetime(errors='coerce')` prevent
  bad data from propagating silently
- Negative counts set to `NaN` (CDC sometimes issues corrections)
- `new_cases` / `new_deaths` imputed from cumulative diff when missing
- 3-day forward-fill for cumulative columns; 7-day for vaccination counts
- `INSERT OR REPLACE` makes the pipeline fully idempotent (safe to re-run)

### Load – Normalised Schema (3NF)
```
dim_state (PK: state_code)
fact_cases          → FK state_code, UNIQUE (report_date, state_code)
fact_vax_state      → FK state_code, UNIQUE (report_date, state_code)
fact_vax_national   →                UNIQUE (report_date)
```
Indexes on `(state_code, report_date)` and `report_date` alone support the
two most common query patterns (state time-series and national aggregations)
with sub-millisecond latency on a local SQLite file.

### Dashboard
- `@st.cache_resource` – single DB connection per session
- `@st.cache_data(ttl=3600)` – query results cached for 1 hour
- All Plotly charts are responsive (`use_container_width=True`)
- Date range + state selector in sidebar filter all views simultaneously

---

## Resume Bullets This Project Supports

- **Engineered an ETL pipeline** to extract and integrate US COVID-19 data from CDC APIs and CSVs → `extract.py`
- **Developed Pandas transformations** to normalize schemas, resolve missing values, and enforce data validation → `transform.py`
- **Designed a normalized SQLite database**, optimizing indexing and SQL queries for low-latency analytics → `load.py`
- **Deployed a Streamlit BI dashboard** to query data and visualize national vaccination trends → `dashboard.py`
