# Nigeria Oil & Gas Production Analytics Dashboard

A data engineering and analytics project tracking Nigerian crude oil production trends, shut-in volumes, and Brent price correlations using public data from NUPRC, OPEC, and EIA.

**Live Demo:** [your-app.streamlit.app](https://your-app.streamlit.app) ← update after deployment  
**Author:** Oluwafemi — [LinkedIn](https://linkedin.com/in/yourprofile) · [Medium](https://medium.com/@yourprofile)

---

## What This Dashboard Shows

| Chart | What It Answers |
|---|---|
| Production trend (2010–present) | Is Nigeria hitting its OPEC quota? |
| Field-level breakdown | Which fields drive national output? |
| Shut-in volume tracker | How much capacity is offline and why? |
| Brent price correlation | Does price movement predict production changes? |
| Year-on-year delta | How fast is Nigeria's output declining or recovering? |

---

## Data Sources

| Source | What | URL | Format |
|---|---|---|---|
| NUPRC | Monthly crude production by field | nuprc.gov.ng | PDF/CSV |
| OPEC | Nigeria production quota & actual | opec.org | CSV |
| EIA | Brent crude spot price | eia.gov/opendata | API (free key) |
| EIA | Nigeria production (barrels/day) | eia.gov/opendata | API (free key) |

All data is publicly available. No proprietary or confidential data is used.

---

## Tech Stack

```
Data Layer         :  Python (requests, pandas) → PostgreSQL
Transformation     :  SQL views (production analytics)
Dashboard          :  Streamlit + Plotly
Deployment         :  Streamlit Community Cloud (free)
```

---

## Project Structure

```
ng_oil_gas_dashboard/
├── data/
│   ├── raw/              # Downloaded CSVs (gitignored)
│   └── processed/        # Cleaned, schema-consistent CSVs
├── sql/
│   ├── schema.sql        # Table definitions
│   └── views.sql         # Analytics views
├── scripts/
│   ├── ingest_eia.py     # EIA API ingestion
│   ├── ingest_nuprc.py   # NUPRC/OPEC CSV ingestion
│   ├── clean.py          # Cleaning & normalisation
│   └── load_db.py        # Load processed data to PostgreSQL
├── dashboard/
│   └── app.py            # Streamlit app (main entry point)
├── docs/
│   └── data_dictionary.md
├── requirements.txt
├── .env.example
└── README.md
```

---

## Getting Started (Local)

### 1. Prerequisites
- Python 3.11+
- PostgreSQL 14+ running locally
- Free EIA API key from: [eia.gov/opendata](https://www.eia.gov/opendata/register.php)

### 2. Clone and install
```bash
git clone https://github.com/yourusername/ng_oil_gas_dashboard.git
cd ng_oil_gas_dashboard
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env — add your EIA API key and database credentials
```

### 4. Set up database
```bash
psql -U postgres -c "CREATE DATABASE ng_oil_gas;"
psql -U postgres -d ng_oil_gas -f sql/schema.sql
psql -U postgres -d ng_oil_gas -f sql/views.sql
```

### 5. Ingest data
```bash
python scripts/ingest_eia.py        # pulls Brent price + NG production
python scripts/ingest_nuprc.py      # loads NUPRC/OPEC CSVs from data/raw/
python scripts/clean.py             # normalises and validates
python scripts/load_db.py           # loads to PostgreSQL
```

### 6. Run dashboard
```bash
streamlit run dashboard/app.py
```
Open [http://localhost:8501](http://localhost:8501)

---

## Deploying to Streamlit Community Cloud (Free)

1. Push your repo to GitHub (make sure `.env` is in `.gitignore`)
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app
3. Connect your GitHub repo, set main file as `dashboard/app.py`
4. Add secrets in the Streamlit Cloud dashboard (Settings → Secrets):
   ```toml
   EIA_API_KEY = "your_key_here"
   DB_HOST = "your_db_host"
   DB_NAME = "ng_oil_gas"
   DB_USER = "your_user"
   DB_PASSWORD = "your_password"
   ```
5. Deploy — your live link appears in ~2 minutes

**Note:** For production, use Supabase (free PostgreSQL in the cloud) instead of a local DB so Streamlit Cloud can connect to it.

---

## Methodology Notes

**Shut-in volume calculation:**  
`shut_in_volume = nameplate_capacity - actual_production`  
Nameplate capacity sourced from NUPRC field licence data and cross-referenced with operator annual reports.

**Price correlation:**  
Pearson correlation coefficient calculated on monthly averages. Rolling 12-month window used to smooth seasonality.

**Data gaps:**  
Some NUPRC field-level data has gaps in 2019–2021 due to force majeure reporting delays. These are flagged as `NULL` rather than interpolated.

---

## Author

Built by Oluwafemi as part of a commodity analytics portfolio targeting data engineering and trading analytics roles.  
Questions or feedback: open an issue or reach out on LinkedIn.