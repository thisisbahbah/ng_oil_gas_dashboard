"""
ingest_eia.py
─────────────
Pulls two EIA data series:
  1. Brent crude spot price (monthly, USD/barrel)
  2. Nigeria crude oil production (monthly, thousand barrels/day)

Writes raw CSVs to data/raw/ for audit trail.
Clean/load step is handled separately by clean.py + load_db.py.

Usage:
    python scripts/ingest_eia.py

Requires:
    EIA_API_KEY in .env
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────
API_KEY   = os.getenv("EIA_API_KEY")
BASE_URL  = "https://api.eia.gov/v2"
DATA_DIR  = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# EIA series IDs
# Brent crude monthly price: PET.RBRTE.M
# Nigeria crude production:  PET.MCREXNI2.M  (thousand barrels/day)
SERIES = {
    "brent_price": {
        "series_id": "PET.RBRTE.M",
        "description": "Brent crude spot price, USD/barrel",
        "output_file": "eia_brent_price_raw.csv",
    },
    "ng_production": {
        "series_id": "PET.MCREXNI2.M",
        "description": "Nigeria crude production, thousand barrels/day",
        "output_file": "eia_ng_production_raw.csv",
    },
}


def fetch_eia_series(series_id: str, start: str = "2010-01") -> pd.DataFrame:
    """
    Fetch a monthly EIA time series using the v2 API.

    Parameters
    ----------
    series_id : str
        EIA series ID (e.g. 'PET.RBRTE.M')
    start : str
        ISO start date 'YYYY-MM', default '2010-01'

    Returns
    -------
    pd.DataFrame with columns: period (YYYY-MM), value
    """
    if not API_KEY:
        raise ValueError(
            "EIA_API_KEY not set. Get a free key at https://www.eia.gov/opendata/register.php"
        )

    # Parse series to category/name for v2 endpoint
    # EIA v2 route: /seriesid/{series_id}
    url = f"{BASE_URL}/seriesid/{series_id}"
    params = {
        "api_key": API_KEY,
        "data[]": "value",
        "start": start,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 5000,   # max per request; enough for monthly data since 2010
    }

    print(f"  Fetching {series_id} ...", end=" ")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if "response" not in data or "data" not in data["response"]:
        raise ValueError(f"Unexpected EIA response structure: {list(data.keys())}")

    records = data["response"]["data"]
    if not records:
        raise ValueError(f"No data returned for series {series_id}")

    df = pd.DataFrame(records)[["period", "value"]]
    df.columns = ["period", "value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    df = df.sort_values("period").reset_index(drop=True)

    print(f"{len(df)} rows ({df['period'].min()} → {df['period'].max()})")
    return df


def main():
    print("=" * 55)
    print("EIA Data Ingestion")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    results = {}
    for name, cfg in SERIES.items():
        print(f"\n[{name.upper()}] {cfg['description']}")
        try:
            df = fetch_eia_series(cfg["series_id"])
            out_path = DATA_DIR / cfg["output_file"]
            df.to_csv(out_path, index=False)
            print(f"  Saved to {out_path}")
            results[name] = df
        except requests.HTTPError as e:
            print(f"  HTTP error: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"  Error: {e}")
            sys.exit(1)

    print("\n" + "=" * 55)
    print("Ingestion complete.")
    print(f"  Brent price rows:      {len(results['brent_price'])}")
    print(f"  NG production rows:    {len(results['ng_production'])}")
    print("\nNext step: python scripts/clean.py")
    print("=" * 55)


if __name__ == "__main__":
    main()