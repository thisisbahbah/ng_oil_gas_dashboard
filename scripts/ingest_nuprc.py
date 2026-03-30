"""
ingest_nuprc.py
───────────────
Loads NUPRC field-level production data and OPEC quota data
from CSV files you have manually downloaded to data/raw/.

WHY MANUAL DOWNLOAD:
  NUPRC does not provide a public API. Their monthly reports are
  published as PDFs or Excel files on nuprc.gov.ng. You need to:
    1. Visit: https://nuprc.gov.ng/production/
    2. Download the monthly production reports
    3. Copy/export to CSV format following the template below
    4. Save to data/raw/nuprc_field_production.csv

  For OPEC quota data:
    1. Visit: https://www.opec.org/opec_web/en/press_room/press_releases.htm
    2. Download production data tables (available as Excel)
    3. Format to match the opec_quotas template
    4. Save to data/raw/opec_quotas.csv

This script validates the format, cleans, and saves to data/processed/.

TEMPLATE — nuprc_field_production.csv:
  production_month, field_name, operator, crude_grade,
  production_kbd, nameplate_kbd, shut_in_reason

TEMPLATE — opec_quotas.csv:
  quota_month, quota_kbd, actual_kbd

Usage:
    python scripts/ingest_nuprc.py
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_RAW  = Path(__file__).parent.parent / "data" / "raw"
DATA_PROC = Path(__file__).parent.parent / "data" / "processed"
DATA_PROC.mkdir(parents=True, exist_ok=True)

# ── Expected columns ──────────────────────────────────────
FIELD_COLS = {
    "production_month": "str",   # YYYY-MM or YYYY-MM-DD
    "field_name":       "str",
    "operator":         "str",
    "crude_grade":      "str",
    "production_kbd":   "float",
    "nameplate_kbd":    "float",
    "shut_in_reason":   "str",   # optional, can be empty
}

OPEC_COLS = {
    "quota_month":  "str",
    "quota_kbd":    "float",
    "actual_kbd":   "float",
}

# ── Sample data ───────────────────────────────────────────
# If you don't have the CSVs yet, this creates a sample file
# so you can test the pipeline end-to-end before real data.
SAMPLE_FIELD_DATA = """production_month,field_name,operator,crude_grade,production_kbd,nameplate_kbd,shut_in_reason
2023-01,Bonny,Shell/SPDC,Bonny Light,178.5,280.0,Pipeline vandalism
2023-01,Forcados,Shell/SPDC,Forcados,112.3,180.0,
2023-01,Qua Iboe,ExxonMobil,Qua Iboe,180.2,210.0,
2023-01,Escravos,Chevron,Escravos,105.4,160.0,Force majeure
2023-01,Bonga,Shell/SPDC,Bonga,85.2,120.0,
2023-02,Bonny,Shell/SPDC,Bonny Light,155.3,280.0,Pipeline vandalism
2023-02,Forcados,Shell/SPDC,Forcados,140.1,180.0,
2023-02,Qua Iboe,ExxonMobil,Qua Iboe,192.5,210.0,
2023-02,Escravos,Chevron,Escravos,120.8,160.0,
2023-02,Bonga,Shell/SPDC,Bonga,92.4,120.0,
2023-03,Bonny,Shell/SPDC,Bonny Light,162.7,280.0,
2023-03,Forcados,Shell/SPDC,Forcados,148.9,180.0,
2023-03,Qua Iboe,ExxonMobil,Qua Iboe,188.3,210.0,
2023-03,Escravos,Chevron,Escravos,135.2,160.0,
2023-03,Bonga,Shell/SPDC,Bonga,98.7,120.0,"""

SAMPLE_OPEC_DATA = """quota_month,quota_kbd,actual_kbd
2023-01,1742.0,1411.0
2023-02,1742.0,1399.0
2023-03,1742.0,1438.0
2023-04,1742.0,1350.0
2023-05,1742.0,1295.0
2023-06,1742.0,1330.0"""


def normalise_month_column(series: pd.Series) -> pd.Series:
    """Convert various date formats to YYYY-MM-DD (first of month)."""
    def parse_one(val):
        val = str(val).strip()
        for fmt in ("%Y-%m", "%Y-%m-%d", "%m/%Y", "%b-%Y", "%B %Y"):
            try:
                return pd.to_datetime(val, format=fmt).replace(day=1)
            except ValueError:
                continue
        # fallback
        try:
            return pd.to_datetime(val).replace(day=1)
        except Exception:
            return None

    return series.apply(parse_one)


def load_or_sample(filepath: Path, sample_data: str, name: str) -> pd.DataFrame:
    """Load CSV if it exists, otherwise write and load the sample."""
    if not filepath.exists():
        print(f"  {filepath.name} not found — creating sample data for testing.")
        print(f"  → Replace with real data from NUPRC/OPEC before production use.")
        filepath.write_text(sample_data)

    df = pd.read_csv(filepath)
    print(f"  Loaded {len(df)} rows from {filepath.name}")
    return df


def process_field_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate field-level production data."""
    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Check required columns
    missing = [c for c in FIELD_COLS if c not in df.columns and c != "shut_in_reason"]
    if missing:
        raise ValueError(f"Missing columns in NUPRC data: {missing}")

    # Normalise month
    df["production_month"] = normalise_month_column(df["production_month"])
    df = df.dropna(subset=["production_month"])

    # Numeric columns
    for col in ["production_kbd", "nameplate_kbd"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # String columns
    for col in ["field_name", "operator", "crude_grade"]:
        df[col] = df[col].astype(str).str.strip().str.title()

    df["shut_in_reason"] = df.get("shut_in_reason", "").fillna("").astype(str).str.strip()

    # Derived: shut_in_kbd
    df["shut_in_kbd"] = (
        df["nameplate_kbd"] - df["production_kbd"]
    ).clip(lower=0)

    # Remove impossible values
    df = df[df["production_kbd"] >= 0]
    df = df[df["production_kbd"] <= 2000]  # sanity: no single field > 2M bbl/d

    df = df.sort_values(["production_month", "field_name"]).reset_index(drop=True)
    return df


def process_opec_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate OPEC quota data."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    missing = [c for c in OPEC_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in OPEC data: {missing}")

    df["quota_month"] = normalise_month_column(df["quota_month"])
    df = df.dropna(subset=["quota_month"])

    for col in ["quota_kbd", "actual_kbd"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("quota_month").reset_index(drop=True)
    return df


def print_summary(df: pd.DataFrame, name: str):
    print(f"\n  [{name}] Summary")
    print(f"    Rows:          {len(df)}")
    if "production_month" in df.columns:
        print(f"    Date range:    {df['production_month'].min().date()} → {df['production_month'].max().date()}")
        print(f"    Fields:        {df['field_name'].nunique()} unique")
        print(f"    Missing prod:  {df['production_kbd'].isna().sum()}")
    elif "quota_month" in df.columns:
        print(f"    Date range:    {df['quota_month'].min().date()} → {df['quota_month'].max().date()}")
        avg_compliance = ((df["actual_kbd"] / df["quota_kbd"]) - 1).mean() * 100
        print(f"    Avg compliance vs quota: {avg_compliance:+.1f}%")


def main():
    print("=" * 55)
    print("NUPRC / OPEC Data Ingestion")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # ── Field-level production ────────────────────────────
    print("\n[1/2] Field-level production data")
    field_raw = load_or_sample(
        DATA_RAW / "nuprc_field_production.csv",
        SAMPLE_FIELD_DATA,
        "NUPRC",
    )
    field_clean = process_field_data(field_raw)
    print_summary(field_clean, "NUPRC Field Production")
    out1 = DATA_PROC / "field_production_clean.csv"
    field_clean.to_csv(out1, index=False)
    print(f"  Saved: {out1}")

    # ── OPEC quotas ───────────────────────────────────────
    print("\n[2/2] OPEC quota data")
    opec_raw = load_or_sample(
        DATA_RAW / "opec_quotas.csv",
        SAMPLE_OPEC_DATA,
        "OPEC",
    )
    opec_clean = process_opec_data(opec_raw)
    print_summary(opec_clean, "OPEC Quotas")
    out2 = DATA_PROC / "opec_quotas_clean.csv"
    opec_clean.to_csv(out2, index=False)
    print(f"  Saved: {out2}")

    print("\n" + "=" * 55)
    print("Ingestion complete.")
    print("\nIMPORTANT: Sample data has been generated.")
    print("Replace data/raw/nuprc_field_production.csv with real")
    print("NUPRC data downloaded from https://nuprc.gov.ng/production/")
    print("\nNext step: python scripts/load_db.py")
    print("=" * 55)


if __name__ == "__main__":
    main()