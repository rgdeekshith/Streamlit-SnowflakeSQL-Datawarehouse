import datetime
import os

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, count_distinct, date_trunc, max as max_, min as min_, sum as sum_


st.set_page_config(page_title="DATAWAREHOUSE Sales Dashboard", layout="wide")


@st.cache_resource
def _session():
    try:
        return get_active_session()
    except Exception:
        pass

    if "snowflake" in st.secrets:
        cfg = dict(st.secrets["snowflake"])
    else:
        cfg = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

    missing = [k for k in ("account", "user", "password") if not cfg.get(k)]
    if missing:
        raise RuntimeError(
            "Missing Snowflake connection settings for local run. Provide Streamlit secrets [snowflake] (recommended) or set SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD."
        )

    return Session.builder.configs(cfg).create()


@st.cache_data(ttl=900)
def _date_bounds():
    s = _session()
    r = (
        s.table("DATAWAREHOUSE.GOLD.FACT_SALES")
        .select(min_(col("ORDER_DATE")).alias("MIN_DATE"), max_(col("ORDER_DATE")).alias("MAX_DATE"))
        .to_pandas()
    )
    return r.loc[0, "MIN_DATE"], r.loc[0, "MAX_DATE"]


@st.cache_data(ttl=3600)
def _countries():
    s = _session()
    pdf = s.table("DATAWAREHOUSE.GOLD.DIM_CUSTOMERS").select(col("COUNTRY")).distinct().sort(col("COUNTRY")).to_pandas()
    return [c for c in pdf["COUNTRY"].dropna().astype(str).tolist()]


@st.cache_data(ttl=3600)
def _categories():
    s = _session()
    pdf = s.table("DATAWAREHOUSE.GOLD.DIM_PRODUCTS").select(col("CATEGORY")).distinct().sort(col("CATEGORY")).to_pandas()
    return [c for c in pdf["CATEGORY"].dropna().astype(str).tolist()]


@st.cache_data(ttl=300)
def _pipeline_health_tables():
    s = _session()

    bronze_pdf = s.sql(
        """
        SELECT
          table_name,
          row_count,
          last_altered
        FROM DATAWAREHOUSE.INFORMATION_SCHEMA.TABLES
        WHERE table_catalog = 'DATAWAREHOUSE'
          AND table_schema = 'BRONZE'
          AND table_name IN (
            'CRM_CUST_INFO','CRM_PRD_INFO','CRM_SALES_DETAILS',
            'ERP_CUST_AZ12','ERP_LOC_A101','ERP_PX_CAT_G1V2'
          )
        ORDER BY table_name
        """
    ).to_pandas()

    silver_pdf = s.sql(
        """
        SELECT * FROM (
          SELECT 'CRM_CUST_INFO' AS table_name, COUNT(*) AS row_count, MAX(DWH_CREATE_DATE) AS last_load_ts FROM DATAWAREHOUSE.SILVER.CRM_CUST_INFO
          UNION ALL
          SELECT 'CRM_PRD_INFO', COUNT(*), MAX(DWH_CREATE_DATE) FROM DATAWAREHOUSE.SILVER.CRM_PRD_INFO
          UNION ALL
          SELECT 'CRM_SALES_DETAILS', COUNT(*), MAX(DWH_CREATE_DATE) FROM DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS
          UNION ALL
          SELECT 'ERP_CUST_AZ12', COUNT(*), MAX(DWH_CREATE_DATE) FROM DATAWAREHOUSE.SILVER.ERP_CUST_AZ12
          UNION ALL
          SELECT 'ERP_LOC_A101', COUNT(*), MAX(DWH_CREATE_DATE) FROM DATAWAREHOUSE.SILVER.ERP_LOC_A101
          UNION ALL
          SELECT 'ERP_PX_CAT_G1V2', COUNT(*), MAX(DWH_CREATE_DATE) FROM DATAWAREHOUSE.SILVER.ERP_PX_CAT_G1V2
        )
        ORDER BY table_name
        """
    ).to_pandas()

    gold_pdf = s.sql(
        """
        SELECT * FROM (
          SELECT 'FACT_SALES' AS object_name, COUNT(*) AS row_count, MAX(ORDER_DATE) AS last_event_date FROM DATAWAREHOUSE.GOLD.FACT_SALES
          UNION ALL
          SELECT 'DIM_CUSTOMERS', COUNT(*), NULL FROM DATAWAREHOUSE.GOLD.DIM_CUSTOMERS
          UNION ALL
          SELECT 'DIM_PRODUCTS', COUNT(*), NULL FROM DATAWAREHOUSE.GOLD.DIM_PRODUCTS
        )
        ORDER BY object_name
        """
    ).to_pandas()

    return bronze_pdf, silver_pdf, gold_pdf


def _base_df():
    s = _session()
    fs = s.table("DATAWAREHOUSE.GOLD.FACT_SALES")
    dc = s.table("DATAWAREHOUSE.GOLD.DIM_CUSTOMERS").select(
        col("CUSTOMER_KEY"),
        col("CUSTOMER_ID"),
        col("FIRST_NAME"),
        col("LAST_NAME"),
        col("COUNTRY"),
    )
    dp = s.table("DATAWAREHOUSE.GOLD.DIM_PRODUCTS").select(
        col("PRODUCT_KEY"),
        col("PRODUCT_ID"),
        col("PRODUCT_NAME"),
        col("CATEGORY"),
        col("SUBCATEGORY"),
    )
    return (
        fs.join(dc, fs["CUSTOMER_KEY"] == dc["CUSTOMER_KEY"], how="left")
        .join(dp, fs["PRODUCT_KEY"] == dp["PRODUCT_KEY"], how="left")
        .select(
            fs["ORDER_NUMBER"].alias("ORDER_NUMBER"),
            fs["ORDER_DATE"].alias("ORDER_DATE"),
            fs["SALES_AMOUNT"].alias("SALES_AMOUNT"),
            fs["QUANTITY"].alias("QUANTITY"),
            dc["CUSTOMER_KEY"].alias("CUSTOMER_KEY"),
            dc["CUSTOMER_ID"].alias("CUSTOMER_ID"),
            dc["FIRST_NAME"].alias("FIRST_NAME"),
            dc["LAST_NAME"].alias("LAST_NAME"),
            dc["COUNTRY"].alias("COUNTRY"),
            dp["PRODUCT_KEY"].alias("PRODUCT_KEY"),
            dp["PRODUCT_ID"].alias("PRODUCT_ID"),
            dp["PRODUCT_NAME"].alias("PRODUCT_NAME"),
            dp["CATEGORY"].alias("CATEGORY"),
            dp["SUBCATEGORY"].alias("SUBCATEGORY"),
        )
    )


st.title("DATAWAREHOUSE Dashboard")

min_date, max_date = _date_bounds()
if min_date is None or max_date is None:
    st.error("No data found in DATAWAREHOUSE.GOLD.FACT_SALES")
    st.stop()

with st.sidebar:
    st.header("Filters")
    default_start = max(min_date, max_date - datetime.timedelta(days=30))
    dr = st.date_input("Order date range", value=(default_start, max_date), min_value=min_date, max_value=max_date)
    if isinstance(dr, tuple) and len(dr) == 2:
        start_date, end_date = dr
    else:
        start_date, end_date = min_date, max_date

    country_options = ["All"] + _countries()
    category_options = ["All"] + _categories()

    country = st.selectbox("Country", options=country_options, index=0)
    category = st.selectbox("Category", options=category_options, index=0)

    trend_grain = st.radio("Trend grain", options=["Daily", "Monthly"], horizontal=True)
    stale_hours = st.number_input("Staleness threshold (hours)", min_value=1, max_value=720, value=24)

base = _base_df().filter((col("ORDER_DATE") >= start_date) & (col("ORDER_DATE") <= end_date))
if country != "All":
    base = base.filter(col("COUNTRY") == country)
if category != "All":
    base = base.filter(col("CATEGORY") == category)

tab_overview, tab_trends, tab_customers, tab_products, tab_health = st.tabs(
    ["Overview", "Trends", "Customers", "Products", "Pipeline Health"]
)

with tab_overview:
    kpi_row = base.agg(
        sum_(col("SALES_AMOUNT")).alias("REVENUE"),
        count_distinct(col("ORDER_NUMBER")).alias("ORDERS"),
        sum_(col("QUANTITY")).alias("UNITS"),
    ).collect()

    revenue = float(kpi_row[0]["REVENUE"] or 0)
    orders = int(kpi_row[0]["ORDERS"] or 0)
    units = float(kpi_row[0]["UNITS"] or 0)
    aov = revenue / orders if orders else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue", f"{revenue:,.2f}")
    c2.metric("Orders", f"{orders:,.0f}")
    c3.metric("Units", f"{units:,.0f}")
    c4.metric("AOV", f"{aov:,.2f}")

    st.caption("Source: DATAWAREHOUSE.GOLD views")

with tab_trends:
    if trend_grain == "Monthly":
        period = date_trunc("month", col("ORDER_DATE")).alias("PERIOD")
    else:
        period = col("ORDER_DATE").alias("PERIOD")

    trend_df = (
        base.select(period, col("ORDER_NUMBER"), col("SALES_AMOUNT"), col("QUANTITY"))
        .group_by(col("PERIOD"))
        .agg(
            sum_(col("SALES_AMOUNT")).alias("REVENUE"),
            count_distinct(col("ORDER_NUMBER")).alias("ORDERS"),
            sum_(col("QUANTITY")).alias("UNITS"),
        )
        .sort(col("PERIOD"))
        .to_pandas()
    )

    if trend_df.empty:
        st.info("No rows for selected filters.")
    else:
        trend_df["AOV"] = trend_df.apply(lambda r: (r["REVENUE"] / r["ORDERS"]) if r["ORDERS"] else 0, axis=1)
        st.line_chart(trend_df.set_index("PERIOD")["REVENUE"])
        st.dataframe(trend_df, use_container_width=True)

with tab_customers:
    cust_pdf = (
        base.group_by(
            col("CUSTOMER_KEY"),
            col("CUSTOMER_ID"),
            col("FIRST_NAME"),
            col("LAST_NAME"),
            col("COUNTRY"),
        )
        .agg(
            sum_(col("SALES_AMOUNT")).alias("REVENUE"),
            count_distinct(col("ORDER_NUMBER")).alias("ORDERS"),
            sum_(col("QUANTITY")).alias("UNITS"),
        )
        .sort(col("REVENUE").desc())
        .limit(20)
        .to_pandas()
    )

    if cust_pdf.empty:
        st.info("No rows for selected filters.")
    else:
        cust_pdf["AOV"] = cust_pdf.apply(lambda r: (r["REVENUE"] / r["ORDERS"]) if r["ORDERS"] else 0, axis=1)
        st.dataframe(cust_pdf, use_container_width=True)
        st.download_button("Download CSV", cust_pdf.to_csv(index=False), file_name="top_customers.csv", mime="text/csv")

with tab_products:
    prod_pdf = (
        base.group_by(
            col("PRODUCT_KEY"),
            col("PRODUCT_ID"),
            col("PRODUCT_NAME"),
            col("CATEGORY"),
            col("SUBCATEGORY"),
        )
        .agg(
            sum_(col("SALES_AMOUNT")).alias("REVENUE"),
            count_distinct(col("ORDER_NUMBER")).alias("ORDERS"),
            sum_(col("QUANTITY")).alias("UNITS"),
        )
        .sort(col("REVENUE").desc())
        .limit(20)
        .to_pandas()
    )

    if prod_pdf.empty:
        st.info("No rows for selected filters.")
    else:
        prod_pdf["AOV"] = prod_pdf.apply(lambda r: (r["REVENUE"] / r["ORDERS"]) if r["ORDERS"] else 0, axis=1)
        st.dataframe(prod_pdf, use_container_width=True)
        st.download_button("Download CSV", prod_pdf.to_csv(index=False), file_name="top_products.csv", mime="text/csv")

with tab_health:
    bronze_pdf, silver_pdf, gold_pdf = _pipeline_health_tables()

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    def _age_hours(ts):
        if ts is None or (isinstance(ts, float) and pd.isna(ts)) or pd.isna(ts):
            return None
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return (now_utc - ts).total_seconds() / 3600

    if not bronze_pdf.empty:
        bronze_pdf["AGE_HOURS"] = bronze_pdf["LAST_ALTERED"].apply(_age_hours)
        bronze_pdf["STATUS"] = bronze_pdf["AGE_HOURS"].apply(lambda h: "STALE" if (h is not None and h > stale_hours) else ("OK" if h is not None else "N/A"))

    if not silver_pdf.empty:
        silver_pdf["AGE_HOURS"] = silver_pdf["LAST_LOAD_TS"].apply(_age_hours)
        silver_pdf["STATUS"] = silver_pdf["AGE_HOURS"].apply(lambda h: "STALE" if (h is not None and h > stale_hours) else ("OK" if h is not None else "N/A"))

    latest_bronze = None if bronze_pdf.empty else bronze_pdf["LAST_ALTERED"].max()
    latest_silver = None if silver_pdf.empty else silver_pdf["LAST_LOAD_TS"].max()
    latest_gold_event = None if gold_pdf.empty else gold_pdf["LAST_EVENT_DATE"].dropna().max()

    stale_bronze = 0 if bronze_pdf.empty else int((bronze_pdf["STATUS"] == "STALE").sum())
    stale_silver = 0 if silver_pdf.empty else int((silver_pdf["STATUS"] == "STALE").sum())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Latest Bronze activity", "N/A" if latest_bronze is None or pd.isna(latest_bronze) else str(latest_bronze))
    m2.metric("Latest Silver load", "N/A" if latest_silver is None or pd.isna(latest_silver) else str(latest_silver))
    m3.metric("Latest Gold order date", "N/A" if latest_gold_event is None or pd.isna(latest_gold_event) else str(latest_gold_event))
    m4.metric("Stale tables", f"{stale_bronze + stale_silver}")

    st.subheader("Bronze")
    st.dataframe(bronze_pdf, use_container_width=True)

    st.subheader("Silver")
    st.dataframe(silver_pdf, use_container_width=True)

    st.subheader("Gold")
    st.dataframe(gold_pdf, use_container_width=True)
