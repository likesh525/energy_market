# Nordic Power Load Pipeline (DK1 / DK2)

A small end-to-end data pipeline that ingests ENTSO-E Total Load data
(actual vs. day-ahead forecast) for the Danish bidding zones DK1 and DK2,
cleans and aligns it, stores it as Parquet, and exposes it for analytical
SQL queries via DuckDB.

## Pipeline stages

1. **Ingest** — read raw CSV exports from the ENTSO-E Transparency Platform
   (Total Load, Day-ahead vs Actual, per bidding zone).
2. **Clean**
   - Parse the `MTU` time range into a single start timestamp
   - Localize to `Europe/Copenhagen` (CET/CEST), correctly handling the
     DST "spring forward" / "fall back" transitions
   - Normalize zone codes (`BZN|DK1` → `DK1`)
   - Coerce numeric columns and flag missing values (ENTSO-E uses `n/e`
     for missing data points)
3. **Transform** — compute forecast error (`actual − day-ahead`) in MW
   and as a percentage, plus derived `date` / `hour` columns for grouping.
4. **Store** — write the combined, cleaned dataset to a single Parquet file.
5. **Query** — load the Parquet file into DuckDB and run analytical
   queries, e.g.:
   - Average forecast error by zone
   - Average forecast error by hour of day
   - Days with the largest absolute forecast error

## Usage

```bash
pip install pandas duckdb pyarrow

# Place raw CSVs (one per zone) in data/
python pipeline.py
```

## Data source

ENTSO-E Transparency Platform — Total Load (Day Ahead / Actual) [6.1.A],
exported per bidding zone (DK1, DK2) [2025, historic data].

## Notes / next steps

- This pipeline currently uses Total Load data (actual vs. day-ahead
  forecast) as a stand-in for the day-ahead **price** pipeline referenced
  elsewhere — the structure (ingest → clean → align actuals vs forecasts →
  Parquet → DuckDB) is identical. Day-ahead price data requires a
  registered ENTSO-E account for API/bulk access; integrating it is the
  natural next extension.
- Could be extended with generation, transmission, and cross-border flow
  data to analyze capacity margins per zone.