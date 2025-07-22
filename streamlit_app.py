import streamlit as st
import pandas as pd
import numpy as np
import requests
import ast
import re
import io
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
import pytz
from collections import defaultdict

# --- AUTH ---
password = st.text_input("🔐Ingrese la contraseña", type="password")
if password != st.secrets["app_password"]:
    st.stop()

# --- CONFIG ---
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}
MADRID_TZ = pytz.timezone("Europe/Madrid")
st.set_page_config(page_title="📦 Análisis de Stock", layout="wide")
st.title("📦 Análisis de Stock (Últimos 6 Meses)")

# --- TIME RANGE ---
now_madrid = datetime.now(MADRID_TZ)
six_months_ago = now_madrid - relativedelta(months=6)
today_ts = int(now_madrid.timestamp())
six_months_ago_ts = int(six_months_ago.timestamp())

# --- FETCH FUNCTIONS ---
@st.cache_data(ttl=3600)
def fetch_docs(doc_type, start=None, end=None):
    url = f"https://api.holded.com/api/invoicing/v1/documents/{doc_type}"
    if start and end:
        url += f"?starttmp={start}&endtmp={end}"
    return pd.DataFrame(requests.get(url, headers=HEADERS).json())

@st.cache_data(ttl=3600)
def fetch_products():
    all_prods, page = [], 1
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

# --- EXTRACT PRODUCT LINES ---
def extract_products(df):
    rows = []
    for _, r in df.iterrows():
        docnum = r.get("docNumber", "UNKNOWN")
        prod = r.get("products")
        if isinstance(prod, str): prod = ast.literal_eval(prod)
        if isinstance(prod, list):
            for item in prod:
                rows.append({
                    "ID": item.get("productId"),
                    "SKU": item.get("sku"),
                    "Product Name": item.get("name"),
                    "Units": item.get("units", 0),
                    "DocNumber": docnum,
                    "RawDate": r.get("date", None)
                })
    return pd.DataFrame(rows)

# --- FIX SKU & NAME ---
def fix_sku_and_name(row):
    if str(row["SKU"]) == "0" or pd.isnull(row["SKU"]):
        name = str(row["Product Name"])
        patterns = [r"^(\d+)\s+(.*)", r"^SKU\s*(\d+)\s+(.*)", r"^(\d+)-\s*(.*)", r"^Ref:\s*(\d+)\s+(.*)"]
        for pattern in patterns:
            match = re.match(pattern, name)
            if match:
                row["SKU"] = match.group(1)
                row["Product Name"] = match.group(2)
                break
    return row

# --- LOAD DATA ---
product_df = fetch_products()
pedido_df = fetch_docs("salesorder", start=six_months_ago_ts, end=today_ts)
albaran_df = fetch_docs("waybill")

pedido_products = extract_products(pedido_df).apply(fix_sku_and_name, axis=1)
albaran_products = extract_products(albaran_df).apply(fix_sku_and_name, axis=1)

pedido_products = pedido_products[pedido_products["SKU"].astype(str) != "0"]
albaran_products = albaran_products[albaran_products["SKU"].astype(str) != "0"]

# --- ADD DATE ---
pedido_products["Date"] = pedido_products["RawDate"].apply(
    lambda ts: datetime.fromtimestamp(ts, UTC).astimezone(MADRID_TZ).date() if pd.notnull(ts) else None
)
pedido_products = pedido_products.dropna(subset=["Date"])

# --- MEDIA LOGIC ---
sku_units_df = pedido_products[["SKU", "Product Name", "Units", "Date"]].copy()
sku_units_df["Month"] = sku_units_df["Date"].apply(lambda d: d.replace(day=1))
month_bins = [(now_madrid - relativedelta(months=i)).date().replace(day=1) for i in range(6)][::-1]
weights = [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]
month_weight_map = dict(zip(month_bins, weights))

grouped = sku_units_df.groupby(["SKU", "Product Name", "Month"]).agg({"Units": "sum"}).reset_index()
grouped["Weight"] = grouped["Month"].map(month_weight_map)
grouped["Weighted Units"] = grouped["Units"] * grouped["Weight"]

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

# --- AGGREGATE FOR STOCK ---
pedido_agg = pedido_products.groupby("SKU", as_index=False).agg({
    "Units": "sum",
    "Product Name": lambda x: x.dropna().iloc[0] if not x.dropna().empty else ""
}).rename(columns={"Units": "Units (Last 6 Months)"})

albaran_agg = albaran_products.groupby("SKU", as_index=False).agg({"Units": "sum"}).rename(columns={"Units": "Units_Shipped"})

merged_df = pd.merge(pedido_agg, albaran_agg, on="SKU", how="left")
merged_df["Units_Shipped"] = merged_df["Units_Shipped"].fillna(0).astype(int)
merged_df["Stock Reservado"] = merged_df["Units (Last 6 Months)"] - merged_df["Units_Shipped"]
merged_df = merged_df[merged_df["Stock Reservado"] > 0]

# --- MAP STOCK REAL ---
product_df["sku"] = product_df["sku"].astype(str)
merged_df["SKU"] = merged_df["SKU"].astype(str)
stock_map = product_df.set_index("sku")["stock"].to_dict()
merged_df["Stock Real"] = merged_df["SKU"].map(stock_map).fillna(0).astype(int)
merged_df["Stock Disponible"] = merged_df["Stock Real"] - merged_df["Stock Reservado"]

# --- FINAL MERGE ---
final_df = pd.merge(summary_df, merged_df, on=["SKU", "Product Name"], how="inner")

# --- CLEANUP & DISPLAY ---
final_df = final_df[[
    "SKU", "Product Name", "Units (Last 6 Months)", "Media Lineal (Mes)", "Media Exponencial (Mes)", "Media",
    "Active Months", "Stock Reservado", "Stock Real", "Stock Disponible"
]].sort_values(by="Units (Last 6 Months)", ascending=False)

# --- SEARCH FIELD ---
search_input = st.text_input("🔍 Buscar por SKU o Nombre del Producto")
filtered_df = final_df.copy()
if search_input:
    search_lower = search_input.lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].str.lower().str.contains(search_lower, na=False) |
        filtered_df["Product Name"].str.lower().str.contains(search_lower, na=False)
    ]

# --- DISPLAY ---
st.markdown(f"### Total Productos: {final_df.shape[0]}")
st.dataframe(filtered_df, use_container_width=True)

# --- DOWNLOAD ---
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="openpyxl") as writer:
    filtered_df.to_excel(writer, index=False)
buf.seek(0)
st.download_button(
    "📥 Descargar Excel (Stock)",
    buf,
    file_name="stock_analysis_6_meses.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
