"""
Nordic Power Load Pipeline (Production-Simulation)
----------------------------------------------------
Fetches (in this case, loads locally-downloaded) ENTSO-E Total Load data
for Danish bidding zones DK1 and DK2, cleans and aligns it, stores it as
optimized Parquet, and exposes it via DuckDB for analytical SQL queries.

Pipeline stages:
  1. Ingest   - read raw CSV exports
  2. Clean    - parse timestamps (CET/CEST, handle DST), fix dtypes, handle missing values
  3. Transform- compute day-ahead forecast error (actual - forecast)
  4. Store    - write partitioned Parquet files
  5. Query    - run analytical SQL via DuckDB
"""

import pandas as pd
import duckdb
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# 1. INGEST
# ---------------------------------------------------------------------------
def load_raw(filepath: Path) -> pd.DataFrame:
    """Load a raw ENTSO-E Total Load CSV export."""
    df = pd.read_csv(filepath)
    return df


# ---------------------------------------------------------------------------
# 2. CLEAN
# ---------------------------------------------------------------------------
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a raw ENTSO-E load dataframe:
      - Parse the MTU range into a proper start timestamp
      - Localize to CET/CEST (Europe/Copenhagen), handling DST transitions
      - Normalize the Area column (BZN|DK1 -> DK1)
      - Coerce numeric columns, flag missing values
    """
    df = df.copy()

    # MTU looks like "01/01/2025 00:00 - 01/01/2025 01:00" -> take the start
    start_str = df["MTU (CET/CEST)"].str.split(" - ").str[0]

    # Remove any trailing timezone abbreviations like " (CET)" or " (CEST)"
   
    start_str = start_str.str.replace(r'\s*\(C(E|ES)T\)$', '', regex=True)

    # Parse as naive datetime first (format is DD/MM/YYYY HH:MM)
    naive_ts = pd.to_datetime(start_str, format="%d/%m/%Y %H:%M")

    # Localize to Europe/Copenhagen, handling DST.
    # ENTSO-E timestamps are wall-clock local time. During the "fall back"
    # DST transition, the same wall-clock hour occurs twice; we resolve
    # ambiguity by inferring from order (ambiguous='infer'), and during the
    # "spring forward" gap, nonexistent times are shifted forward.
    df["timestamp"] = naive_ts.dt.tz_localize(
        "Europe/Copenhagen", ambiguous="infer", nonexistent="shift_forward"
    )

    # Normalize zone code: "BZN|DK1" -> "DK1"
    df["zone"] = df["Area"].str.replace("BZN|", "", regex=False)

    # Coerce numeric columns (ENTSO-E sometimes uses "n/e" for missing values)
    for col in ["Actual Total Load (MW)", "Day-ahead Total Load Forecast (MW)"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.rename(
        columns={
            "Actual Total Load (MW)": "actual_load_mw",
            "Day-ahead Total Load Forecast (MW)": "forecast_load_mw",
        }
    )[["timestamp", "zone", "actual_load_mw", "forecast_load_mw"]]

    n_missing = df[["actual_load_mw", "forecast_load_mw"]].isna().sum().sum()
    if n_missing:
        print(f"  [clean] {n_missing} missing values found in {df['zone'].iloc[0]}")

    return df


# ---------------------------------------------------------------------------
# 3. TRANSFORM
# ---------------------------------------------------------------------------
def add_forecast_error(df: pd.DataFrame) -> pd.DataFrame:
    """Compute forecast error = actual - day-ahead forecast (and % error)."""
    df = df.copy()
    df["forecast_error_mw"] = df["actual_load_mw"] - df["forecast_load_mw"]
    df["forecast_error_pct"] = (
        df["forecast_error_mw"] / df["forecast_load_mw"]
    ) * 100
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    return df


# ---------------------------------------------------------------------------
# 4. STORE
# ---------------------------------------------------------------------------
def write_parquet(df: pd.DataFrame, path: Path):
    df.to_parquet(path, index=False)
    print(f"  [store] wrote {len(df):,} rows -> {path}")


# ---------------------------------------------------------------------------
# 5. QUERY (DuckDB)
# ---------------------------------------------------------------------------
def run_queries(parquet_path: Path):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW load_data AS SELECT * FROM read_parquet('{parquet_path}')")

    print("\n=== Row count by zone ===")
    print(con.execute("SELECT zone, COUNT(*) AS rows FROM load_data GROUP BY zone").df())

    print("\n=== Average forecast error (MW) by zone ===")
    print(
        con.execute(
            """
            SELECT zone,
                   ROUND(AVG(forecast_error_mw), 2) AS avg_error_mw,
                   ROUND(AVG(ABS(forecast_error_mw)), 2) AS avg_abs_error_mw,
                   ROUND(AVG(forecast_error_pct), 3) AS avg_error_pct
            FROM load_data
            GROUP BY zone
            """
        ).df()
    )

    print("\n=== Average forecast error by hour of day (DK1) ===")
    print(
        con.execute(
            """
            SELECT hour,
                   ROUND(AVG(forecast_error_mw), 2) AS avg_error_mw
            FROM load_data
            WHERE zone = 'DK1'
            GROUP BY hour
            ORDER BY hour
            """
        ).df()
    )

    print("\n=== Top 5 days with largest absolute forecast error (DK1) ===")
    print(
        con.execute(
            """
            SELECT date,
                   ROUND(SUM(ABS(forecast_error_mw)), 2) AS total_abs_error_mw
            FROM load_data
            WHERE zone = 'DK1'
            GROUP BY date
            ORDER BY total_abs_error_mw DESC
            LIMIT 5
            """
        ).df()
    )

    con.close()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    frames = []
    for csv_file in sorted(DATA_DIR.glob("*.csv")):
        print(f"[ingest] reading {csv_file.name}")
        raw = load_raw(csv_file)
        cleaned = clean(raw)
        frames.append(cleaned)

    combined = pd.concat(frames, ignore_index=True)
    combined = add_forecast_error(combined)
    combined = combined.sort_values(["zone", "timestamp"]).reset_index(drop=True)

    parquet_path = OUTPUT_DIR / "dk_load_2025.parquet"
    write_parquet(combined, parquet_path)

    run_queries(parquet_path)


if __name__ == "__main__":
    main()