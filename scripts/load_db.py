"""
load_db.py
──────────
Loads all cleaned CSVs from data/processed/ into PostgreSQL.

Uses upsert logic (INSERT ... ON CONFLICT DO UPDATE) so you can
re-run safely without creating duplicate rows.

Usage:
    python scripts/load_db.py

Requires:
    - PostgreSQL running
    - DB credentials in .env
    - schema.sql already run: psql -d ng_oil_gas -f sql/schema.sql
    - clean data in data/processed/ (run ingest_eia.py + ingest_nuprc.py first)
"""

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATA_PROC = Path(__file__).parent.parent / "data" / "processed"

# ── Build connection string ───────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ng_oil_gas")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "")
CONN_STR = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    try:
        engine = create_engine(CONN_STR)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"  Connected: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        return engine
    except Exception as e:
        print(f"  DB connection failed: {e}")
        print(f"  Check .env credentials and that PostgreSQL is running.")
        sys.exit(1)


def load_brent_prices(engine, df: pd.DataFrame) -> int:
    """Upsert Brent price data."""
    df = df.rename(columns={"period": "price_date", "value": "price_usd"})
    df["price_date"] = pd.to_datetime(df["price_date"] + "-01")  # YYYY-MM → date
    df["source"] = "EIA"

    upsert_sql = text("""
        INSERT INTO brent_prices (price_date, price_usd, source)
        VALUES (:price_date, :price_usd, :source)
        ON CONFLICT (price_date) DO UPDATE
            SET price_usd = EXCLUDED.price_usd,
                source    = EXCLUDED.source
    """)
    with engine.begin() as conn:
        records = df.to_dict(orient="records")
        conn.execute(upsert_sql, records)
    return len(records)


def load_national_production(engine, df: pd.DataFrame) -> int:
    """Upsert national production (EIA series)."""
    df = df.rename(columns={"period": "production_month", "value": "production_kbd"})
    df["production_month"] = pd.to_datetime(df["production_month"] + "-01")
    df["source"] = "EIA"

    upsert_sql = text("""
        INSERT INTO national_production (production_month, production_kbd, source)
        VALUES (:production_month, :production_kbd, :source)
        ON CONFLICT (production_month) DO UPDATE
            SET production_kbd = EXCLUDED.production_kbd,
                source         = EXCLUDED.source
    """)
    with engine.begin() as conn:
        records = df.to_dict(orient="records")
        conn.execute(upsert_sql, records)
    return len(records)


def load_opec_quotas(engine, df: pd.DataFrame) -> int:
    """Upsert OPEC quota data."""
    df["quota_month"] = pd.to_datetime(df["quota_month"])
    df["source"] = "OPEC"

    upsert_sql = text("""
        INSERT INTO opec_quotas (quota_month, quota_kbd, actual_kbd, source)
        VALUES (:quota_month, :quota_kbd, :actual_kbd, :source)
        ON CONFLICT (quota_month) DO UPDATE
            SET quota_kbd  = EXCLUDED.quota_kbd,
                actual_kbd = EXCLUDED.actual_kbd,
                source     = EXCLUDED.source
    """)
    with engine.begin() as conn:
        records = df[["quota_month", "quota_kbd", "actual_kbd", "source"]].to_dict(orient="records")
        conn.execute(upsert_sql, records)
    return len(records)


def load_field_production(engine, df: pd.DataFrame) -> int:
    """Upsert field-level production data."""
    df["production_month"] = pd.to_datetime(df["production_month"])
    df["source"] = "NUPRC"
    df["shut_in_reason"] = df["shut_in_reason"].where(df["shut_in_reason"] != "", None)

    # Replace float NaN with None so PostgreSQL stores proper NULL.
    # pandas NaN becomes PostgreSQL 'NaN'::numeric which is NOT the same
    # as NULL — this breaks all IS NOT NULL checks in the views.
    for col in ["nameplate_kbd", "production_kbd"]:
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), other=None)

    upsert_sql = text("""
        INSERT INTO production_by_field
            (production_month, field_name, operator, crude_grade,
             production_kbd, nameplate_kbd, shut_in_reason, source)
        VALUES
            (:production_month, :field_name, :operator, :crude_grade,
             :production_kbd, :nameplate_kbd, :shut_in_reason, :source)
        ON CONFLICT (production_month, field_name) DO UPDATE
            SET operator        = EXCLUDED.operator,
                crude_grade     = EXCLUDED.crude_grade,
                production_kbd  = EXCLUDED.production_kbd,
                nameplate_kbd   = EXCLUDED.nameplate_kbd,
                shut_in_reason  = EXCLUDED.shut_in_reason,
                source          = EXCLUDED.source
    """)
    cols = ["production_month", "field_name", "operator", "crude_grade",
            "production_kbd", "nameplate_kbd", "shut_in_reason", "source"]
    with engine.begin() as conn:
        records = df[cols].to_dict(orient="records")
        conn.execute(upsert_sql, records)
    return len(records)


def verify_load(engine):
    """Print row counts for all tables after load."""
    tables = ["brent_prices", "national_production", "opec_quotas", "production_by_field"]
    print("\n  Post-load row counts:")
    with engine.connect() as conn:
        for t in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"    {t:<30} {result:>6} rows")


def main():
    print("=" * 55)
    print("Database Load")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    engine = get_engine()

    # ── Brent prices ──────────────────────────────────────
    print("\n[1/4] Loading Brent prices...")
    brent_path = DATA_PROC / "eia_brent_price_clean.csv"
    if not brent_path.exists():
        # fall back to raw if clean not separate
        brent_path = Path(__file__).parent.parent / "data" / "raw" / "eia_brent_price_raw.csv"
    if brent_path.exists():
        df = pd.read_csv(brent_path)
        n = load_brent_prices(engine, df)
        print(f"  Upserted {n} rows")
    else:
        print(f"  File not found: {brent_path}")
        print("  Run ingest_eia.py first.")

    # ── National production ───────────────────────────────
    print("\n[2/4] Loading national production (EIA)...")
    prod_path = DATA_PROC / "eia_ng_production_clean.csv"
    if not prod_path.exists():
        prod_path = Path(__file__).parent.parent / "data" / "raw" / "eia_ng_production_raw.csv"
    if prod_path.exists():
        df = pd.read_csv(prod_path)
        n = load_national_production(engine, df)
        print(f"  Upserted {n} rows")
    else:
        print(f"  File not found: {prod_path}")

    # ── OPEC quotas ───────────────────────────────────────
    print("\n[3/4] Loading OPEC quotas...")
    opec_path = DATA_PROC / "opec_quotas_clean.csv"
    if opec_path.exists():
        df = pd.read_csv(opec_path)
        n = load_opec_quotas(engine, df)
        print(f"  Upserted {n} rows")
    else:
        print(f"  File not found: {opec_path}")

    # ── Field-level production ────────────────────────────
    print("\n[4/4] Loading field-level production (NUPRC)...")
    field_path = DATA_PROC / "field_production_clean.csv"
    if field_path.exists():
        df = pd.read_csv(field_path)
        n = load_field_production(engine, df)
        print(f"  Upserted {n} rows")
    else:
        print(f"  File not found: {field_path}")

    verify_load(engine)

    print("\n" + "=" * 55)
    print("Load complete. Next step: streamlit run dashboard/app.py")
    print("=" * 55)


if __name__ == "__main__":
    main()