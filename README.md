# US COVID-19 Data Engineering Pipeline

A production-style ETL pipeline for extracting, transforming, and analyzing US COVID-19 data from CDC APIs. Cleaned data is stored in a normalized SQLite database and visualized through a Streamlit dashboard.

## Features

- Extracts COVID-19 case, death, and vaccination data from CDC APIs
- Falls back to local CSV data when APIs are unavailable
- Cleans and validates raw data with Pandas
- Loads transformed data into a normalized SQLite database
- Provides an interactive Streamlit dashboard for analytics

## Project Structure

```bash
covid_pipeline/
├── extract.py
├── transform.py
├── load.py
├── pipeline.py
├── dashboard.py
├── generate_fallback_data.py
├── requirements.txt
└── data/
    ├── fallback/
    └── covid.db
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### 1. (Optional) Generate fallback data

```bash
python generate_fallback_data.py
```

### 2. Run the ETL pipeline

```bash
python pipeline.py
```

This creates the SQLite database at:

```bash
data/covid.db
```

### 3. Launch the dashboard

```bash
streamlit run dashboard.py
```

## Data Sources

- Cases & Deaths by State — CDC Socrata API
- Vaccinations by State — CDC Socrata API
- National Vaccinations — CDC Socrata API

## Tech Stack

- Python
- Pandas
- SQLite
- Streamlit
- Plotly
- Requests

