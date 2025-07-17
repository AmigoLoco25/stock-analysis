import streamlit as st
import pandas as pd
import numpy as np
import requests
import ast
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
import pytz
import re
import io


password = st.text_input("🔐Ingrese la contraseña", type="password")
if password != st.secrets["app_password"]:
    st.stop()


# --- CONFIG ---
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}
MADRID_TZ = pytz.timezone('Europe/Madrid')

st.set_page_config(page_title="📦 Análisis de Stock", layout="wide")
st.title("📦 Análisis de Stock (Últimos 6 Meses)")

# --- FETCH PRODUCTS ---
@st.cache_data(ttl=3600)
def fetch_products():
    all_prods = []
    page = 1
    while True:
        resp = requests.get("https://api.holded.com/api/invoicing/v1/products", headers=HEADERS, params={"page": page})
        resp.raise_for_status()
        data = resp.json()
        batch = data if isinstance(data, list) else data.get("items", [])
        if not batch:
            break
        all_prods.extend(batch)
        page += 1
    return pd.DataFrame(all_prods)

# --- FETCH SALES ORDERS ---
@st.cache_data(ttl=3600)
def fetch_orders():
    url = "https://api.holded.com/api/invoicing/v1/documents/salesorder"
    resp = requests.get(url, headers=HEADERS)
    return pd.DataFrame(resp.json())

# --- FORMAT ORDER DATA ---
def expand_order_rows(df):
    rows = []
    for _, row in df.iterrows():
        date_val = row["date"]
        try:
            products = ast.literal_eval(row["products"]) if isinstance(row["products"], str) else row["products"]
        except:
            continue
        readable_date = datetime.fromtimestamp(date_val, UTC).astimezone(MADRID_TZ).date()
        for item in products:
            rows.append({
                "SKU": item.get("sku"),
                "Product Name": item.get("name"),
                "Units": item.get("units"),
                "Date": readable_date,
            })
    return pd.DataFrame(rows)

# --- SKU & NAME CLEANING ---
def fix_sku_and_name(row):
    if str(row["SKU"]) == "0" or pd.isnull(row["SKU"]):
        name = str(row["Product Name"])
        patterns = [
            r"^(\d+)\s+(.*)", r"^SKU\s*(\d+)\s+(.*)", r"^(\d+)-\s*(.*)", r"^Ref:\s*(\d+)\s+(.*)"
        ]
        for pattern in patterns:
            match = re.match(pattern, name)
            if match:
                row["SKU"] = match.group(1)
                row["Product Name"] = match.group(2)
                break
    return row

# --- START ---
product_df = fetch_products()
order_df = fetch_orders()
sku_units_df = expand_order_rows(order_df)
sku_units_df = sku_units_df.dropna(subset=["Date", "SKU", "Units"])
sku_units_df = sku_units_df.apply(fix_sku_and_name, axis=1)

# --- FILTER LAST 6 MONTHS ---
today = datetime.now(MADRID_TZ).date()
sku_units_df = sku_units_df[sku_units_df["Date"] >= today - timedelta(days=180)]

# --- MONTHLY AGGREGATION ---
sku_units_df["Month"] = sku_units_df["Date"].apply(lambda d: d.replace(day=1))
month_bins = [(today - relativedelta(months=i)).replace(day=1) for i in range(6)][::-1]
weights = [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]
month_weight_map = dict(zip(month_bins, weights))

grouped = sku_units_df.groupby(["SKU", "Product Name", "Month"]).agg({"Units": "sum"}).reset_index()
grouped["Weight"] = grouped["Month"].map(month_weight_map)
grouped["Weighted Units"] = grouped["Units"] * grouped["Weight"]

# --- SUMMARY ---
summary_df = (
    grouped.groupby(["SKU", "Product Name"])
    .agg({
        "Units": "sum",
        "Weighted Units": "sum"
    })
    .rename(columns={"Weighted Units": "Media Exponencial (Mes)"})
    .reset_index()
)

summary_df["Media Lineal (Mes)"] = (summary_df["Units"] / 6).round(2)
summary_df["Media Exponencial (Mes)"] = summary_df["Media Exponencial (Mes)"].round(2)
summary_df["Media"] = ((summary_df["Media Lineal (Mes)"] + summary_df["Media Exponencial (Mes)"]) / 2).round(2)

# --- ACTIVE MONTHS ---
sku_units_df["YearMonth"] = sku_units_df["Date"].values.astype("datetime64[M]")
active_months_df = sku_units_df.groupby(["SKU", "Product Name"])["YearMonth"].nunique().reset_index(name="Active Months")
summary_df = summary_df.merge(active_months_df, on=["SKU", "Product Name"], how="inner")

# --- MERGE STOCK ---
product_df["sku"] = product_df["sku"].astype(str)
summary_df["SKU"] = summary_df["SKU"].astype(str)
stock_map = product_df.set_index("sku")["stock"].to_dict()
summary_df["Stock Real"] = summary_df["SKU"].map(stock_map).fillna(0).astype(int)

# --- REORDER COLUMNS ---
summary_df = summary_df.rename(columns={"Units": "Units (Last 6 Months)"})
summary_df = summary_df.sort_values(by="Units (Last 6 Months)", ascending=False)

cols = summary_df.columns.tolist()
cols.insert(cols.index("Units (Last 6 Months)"), cols.pop(cols.index("Stock Real")))
summary_df = summary_df[cols]

# --- DISPLAY ---
st.markdown(f"### Total Products: {summary_df.shape[0]}")
st.dataframe(summary_df)

# --- DOWNLOAD ---
buf1 = io.BytesIO()
with pd.ExcelWriter(buf1, engine="openpyxl") as w:
    summary_df.to_excel(w, index=False)
buf1.seek(0)
st.download_button(
    "📥 Download Excel (Stock)",
    buf1,
    file_name="product_stock_analysis(6 meses).xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
   )
