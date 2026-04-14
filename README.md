# DATAWAREHOUSE Streamlit Dashboard

This repo contains a Streamlit dashboard that reads from your Snowflake **DATAWAREHOUSE** medallion architecture (Gold + a Pipeline Health tab that checks Bronze/Silver/Gold freshness).

## What you get
- Single-page Streamlit app with tabs: **Overview, Trends, Customers, Products, Pipeline Health**
- Sidebar filters: **order date range**, **country**, **category**, **trend grain**, and **staleness threshold**

## Prerequisites
### 1) Data in Snowflake
The app expects these objects to exist in Snowflake:

Gold views (analytics):
- DATAWAREHOUSE.GOLD.FACT_SALES
- DATAWAREHOUSE.GOLD.DIM_CUSTOMERS
- DATAWAREHOUSE.GOLD.DIM_PRODUCTS

Bronze/Silver tables (pipeline health):
- DATAWAREHOUSE.BRONZE: CRM_CUST_INFO, CRM_PRD_INFO, CRM_SALES_DETAILS, ERP_CUST_AZ12, ERP_LOC_A101, ERP_PX_CAT_G1V2
- DATAWAREHOUSE.SILVER: CRM_CUST_INFO, CRM_PRD_INFO, CRM_SALES_DETAILS, ERP_CUST_AZ12, ERP_LOC_A101, ERP_PX_CAT_G1V2

### 2) Python
- Python 3.10+ recommended

## Required libraries
Install these packages locally:
- streamlit
- pandas
- snowflake-snowpark-python

## Repo files
- `datawarehouse_dashboard.py` — the Streamlit app
- `README.md` — this guide

## Run locally (newcomer-friendly)
### Step 1 — Clone the repo
```bash
git clone <your-github-repo-url>
cd <your-repo-folder>
```

### Step 2 — Create and activate a virtual environment
```bash
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows (PowerShell)
# .venv\Scripts\Activate.ps1
```

### Step 3 — Install dependencies
```bash
pip install streamlit pandas snowflake-snowpark-python
```

### Step 4 — Configure Snowflake credentials (DO NOT COMMIT THESE)
Create a file:

`.streamlit/secrets.toml`

with contents:
```toml
[snowflake]
account = "<your_account_identifier>"
user = "<your_username>"
password = "<your_password>"
role = "<your_role_name>"
warehouse = "<your_warehouse_name>"
database = "<your_database_name>"
schema = "<your_schema_name>"
```

Notes:
- Keep this file **local only**. Add `.streamlit/secrets.toml` to `.gitignore`.
- If you prefer environment variables instead of `secrets.toml`, the app also supports:
  - `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

### Step 5 — Start the dashboard
```bash
streamlit run datawarehouse_dashboard.py
```

Streamlit will print a local URL (usually http://localhost:8501). Open it in your browser.

## How the dashboard refreshes
- The dashboard reads from **Gold**. If new data lands in **Bronze**, it will show up only after your Silver/Gold pipeline runs.
- Some lookups are cached in the app (for performance). You can refresh by rerunning the app or waiting for cache TTLs.

## Publish to GitHub (high-level steps)
1. Create a new GitHub repository.
2. Add `datawarehouse_dashboard.py` and `README.md`.
3. Add a `.gitignore` that excludes:
   - `.venv/`
   - `.streamlit/secrets.toml`
4. Commit and push.

## Troubleshooting
- “Missing Snowflake connection settings”: your `secrets.toml` or env vars are not set.
- “Object does not exist or not authorized”: your role needs access to the DATAWAREHOUSE schemas/objects.
- Slow charts: narrow your date range; avoid very large result sets.

## Acknowledgemts/Credits
I have taken the help of Snowflake’s native AI coding agent designed to streamline data engineering for script design and ensuring the compatibility within the snowflake environment.
