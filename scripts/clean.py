"""
clean.py
────────
Cleans and validates raw EIA CSV files downloaded by ingest_eia.py.
Writes cleaned versions to data/processed/ ready for load_db.py.

What it does:
  - Validates column names and data types
  - Parses and standardises date formats
  - Removes nulls and impossible values
  - Logs every row dropped and why
  - Saves clean CSVs to data/processed/

Usage:
    python scripts/clean.py

Run after:   python scripts/ingest_eia.py
Run before:  python scripts/load_db.py
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR  = Path(__file__).parent.parent / "data" / "raw"
PROC_DIR = Path(__file__).parent.parent / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────

def log(msg: str):
    print(f"  {msg}")


def clean_eia_series(
    filepath: Path,
    output_name: str,
    value_col_rename: str,
    min_val: float,
    max_val: float,
    description: str,
) -> pd.DataFrame:
    """
    Generic cleaner for any raw EIA CSV (period, value format).

    Parameters
    ----------
    filepath        : path to the raw CSV
    output_name     : filename to save in data/processed/
    value_col_rename: what to rename the 'value' column to
    min_val         : minimum plausible value (rows below this are dropped)
    max_val         : maximum plausible value (rows above this are dropped)
    description     : human-readable name for log messages

    Returns
    -------
    cleaned DataFrame
    """
    if not filepath.exists():
        raise FileNotFoundError(
            f"{filepath.name} not found.\n"
            f"Run 'python scripts/ingest_eia.py' first."
        )

    df = pd.read_csv(filepath)
    original_count = len(df)
    log(f"Loaded {original_count} rows from {filepath.name}")

    # ── 1. Validate expected columns exist ───────────────────────
    expected = {"period", "value"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing columns in {filepath.name}: {missing}\n"
            f"Found columns: {list(df.columns)}\n"
            f"Re-run ingest_eia.py to regenerate the raw file."
        )

    # ── 2. Parse dates ────────────────────────────────────────────
    # EIA returns dates as "YYYY-MM" strings.
    # We convert to a proper date (first day of the month).
    df["period"] = pd.to_datetime(df["period"] + "-01", format="%Y-%m-%d", errors="coerce")
    bad_dates = df["period"].isna().sum()
    if bad_dates > 0:
        log(f"  Dropped {bad_dates} rows with unparseable dates")
        df = df.dropna(subset=["period"])

    # ── 3. Parse and validate numeric value ──────────────────────
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    nulls = df["value"].isna().sum()
    if nulls > 0:
        log(f"  Dropped {nulls} rows where value was not numeric")
        df = df.dropna(subset=["value"])

    # ── 4. Range validation ───────────────────────────────────────
    too_low  = (df["value"] < min_val).sum()
    too_high = (df["value"] > max_val).sum()
    if too_low > 0:
        log(f"  Dropped {too_low} rows below minimum ({min_val}) — likely data errors")
        df = df[df["value"] >= min_val]
    if too_high > 0:
        log(f"  Dropped {too_high} rows above maximum ({max_val}) — likely data errors")
        df = df[df["value"] <= max_val]

    # ── 5. Remove duplicates ──────────────────────────────────────
    dupes = df.duplicated(subset=["period"]).sum()
    if dupes > 0:
        log(f"  Dropped {dupes} duplicate date rows (kept first occurrence)")
        df = df.drop_duplicates(subset=["period"], keep="first")

    # ── 6. Sort chronologically ───────────────────────────────────
    df = df.sort_values("period").reset_index(drop=True)

    # ── 7. Rename value column ────────────────────────────────────
    df = df.rename(columns={"value": value_col_rename})

    # ── 8. Summary ────────────────────────────────────────────────
    kept = len(df)
    dropped = original_count - kept
    log(f"  Kept {kept} rows  ({dropped} dropped)")
    log(f"  Date range: {df['period'].min().date()} → {df['period'].max().date()}")
    log(f"  {value_col_rename}: min={df[value_col_rename].min():.2f}  "
        f"max={df[value_col_rename].max():.2f}  "
        f"mean={df[value_col_rename].mean():.2f}")

    # ── 9. Save ───────────────────────────────────────────────────
    out_path = PROC_DIR / output_name
    df.to_csv(out_path, index=False)
    log(f"  Saved → {out_path}")

    return df


# ── Main ──────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("EIA Data Cleaning")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # ── Brent crude price ─────────────────────────────────────────
    print("\n[1/2] Brent crude spot price (USD/barrel)")
    brent = clean_eia_series(
        filepath        = RAW_DIR / "eia_brent_price_raw.csv",
        output_name     = "eia_brent_price_clean.csv",
        value_col_rename= "price_usd",
        min_val         = 5.0,    # Brent has never traded below $5
        max_val         = 250.0,  # Brent has never exceeded $150 — $250 gives headroom
        description     = "Brent price",
    )

    # ── Nigeria crude production ──────────────────────────────────
    print("\n[2/2] Nigeria crude production (thousand barrels/day)")
    production = clean_eia_series(
        filepath        = RAW_DIR / "eia_ng_production_raw.csv",
        output_name     = "eia_ng_production_clean.csv",
        value_col_rename= "production_kbd",
        min_val         = 100.0,  # Nigeria has never produced below 100 kbd
        max_val         = 3000.0, # Nigeria's all-time peak was ~2,500 kbd
        description     = "NG production",
    )

    # ── Final summary ─────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("Cleaning complete.")
    print(f"  Brent price rows:      {len(brent)}")
    print(f"  NG production rows:    {len(production)}")
    print(f"\nCleaned files saved to: data/processed/")
    print("\nNext step: python scripts/load_db.py")
    print("=" * 55)


if __name__ == "__main__":
    main()