import streamlit as st
import pandas as pd
import numpy as np
import requests
import ast
from datetime import datetime, UTC
from dateutil.relativedelta import relativedelta
import pytz
import re
import io

# --- AUTH ---
password = st.text_input("🔐Ingrese la contraseña", type="password")
if password != st.secrets["app_password"]:
    st.stop()

# --- CONFIG ---
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}
BASE_URL = "https://api.holded.com/api/invoicing/v1"
MADRID_TZ = pytz.timezone("Europe/Madrid")

st.set_page_config(page_title="📦 Análisis de Stock", layout="wide")
st.title("📦 Análisis de Stock (Últimos 6 Meses)")

# --- TIMESTAMP RANGE ---
now = datetime.now(MADRID_TZ)
six_months_ago = now - relativedelta(months=6)
start_ts = int(six_months_ago.timestamp())
end_ts = int(now.timestamp())

# --- SKU & NAME CLEANING ---
def fix_sku_and_name(row):
    sku = str(row["SKU"]).strip()
    name = str(row["Product Name"]).strip()
    if sku == "0" or sku.lower() in ["none", "nan"]:
        patterns = [
            r"^(\d+)\s+(.*)", r"^SKU\s*(\d+)\s+(.*)",
            r"^(\d+)-\s*(.*)", r"^Ref:\s*(\d+)\s+(.*)"
        ]
        for pattern in patterns:
            match = re.match(pattern, name)
            if match:
                row["SKU"] = match.group(1)
                row["Product Name"] = match.group(2).strip()
                break
    else:
        row["SKU"] = sku
        row["Product Name"] = name
    return row

# --- FETCH STOCK PRODUCTS ---
@st.cache_data(ttl=3600)
def fetch_products():
    all_prods = []
    page = 1
    while True:
        resp = requests.get(f"{BASE_URL}/products", headers=HEADERS, params={"page": page})
        data = resp.json()
        batch = data if isinstance(data, list) else data.get("items", [])
        if not batch:
            break
        all_prods.extend(batch)
        page += 1
    return pd.DataFrame(all_prods)

# --- FETCH SALES ORDERS ---
@st.cache_data(ttl=3600)
def fetch_salesorders():
    url = f"{BASE_URL}/documents/salesorder?starttmp={start_ts}&endtmp={end_ts}"
    resp = requests.get(url, headers=HEADERS)
    df = pd.DataFrame(resp.json())
    return df[df["docNumber"].str.startswith("SO", na=False)]

# --- FETCH SHIPPED ITEMS ---
def get_shipped_items(doc_id, doc_number):
    url = f"{BASE_URL}/documents/salesorder/{doc_id}/shippeditems"
    try:
        res = requests.get(url, headers=HEADERS)
        res.raise_for_status()
        return [
            {
                "SKU": item.get("sku"),
                "Product Name": item.get("name"),
                "Units_Ordered": item.get("total", 0),
                "Units_Shipped": item.get("sent", 0),
                "Units_Pending": item.get("pending", 0),
            }
            for item in res.json()
        ]
    except:
        return []

# --- MAIN PROCESSING ---
product_df = fetch_products()
sales_df = fetch_salesorders()

shipped_rows = []
for _, row in sales_df.iterrows():
    shipped_rows += get_shipped_items(row["id"], row["docNumber"])

df = pd.DataFrame(shipped_rows)
df = df[df["SKU"].astype(str) != "0"]
df = df.apply(fix_sku_and_name, axis=1)

# --- Aggregate by SKU ---
df = (
    df.groupby("SKU", as_index=False)
    .agg({
        "Product Name": lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0],
        "Units_Ordered": "sum",
        "Units_Shipped": "sum",
        "Units_Pending": "sum"
    })
    .rename(columns={
        "Units_Ordered": "Units (Last 6 Months)",
        "Units_Pending": "Stock Reservado"
    })
)

# --- Add Stock Real ---
product_df["sku"] = product_df["sku"].astype(str)
df["SKU"] = df["SKU"].astype(str)
stock_map = product_df.set_index("sku")["stock"].to_dict()
df["Stock Real"] = df["SKU"].map(stock_map).fillna(0).astype(int)

# --- Compute Stock Disponible ---
df["Stock Disponible"] = df["Stock Real"] - df["Stock Reservado"]

# --- Simulate Weighted Averages ---
# Weights: Last 3 months higher (0.25), others (0.125)
weights = [0.125]*4 + [0.25]*2
df["Media Lineal (Mes)"] = (df["Units (Last 6 Months)"] / 6).round(2)
df["Media Exponencial (Mes)"] = (df["Units (Last 6 Months)"] * sum(weights)/6).round(2)
df["Media"] = ((df["Media Lineal (Mes)"] + df["Media Exponencial (Mes)"]) / 2).round(2)

# --- Active Months (set to 6 if has sales) ---
df["Active Months"] = df["Units (Last 6 Months)"].apply(lambda x: 6 if x > 0 else 0)

# --- FINAL TABLE ---
df = df.sort_values(by="Units (Last 6 Months)", ascending=False)
cols = ["SKU", "Product Name", "Units (Last 6 Months)", "Stock Real", "Stock Reservado", "Stock Disponible",
        "Media Lineal (Mes)", "Media Exponencial (Mes)", "Media", "Active Months"]
df = df[cols]

# --- SEARCH + DISPLAY ---
search_input = st.text_input("🔍 Buscar por SKU o Nombre del Producto")
filtered_df = df.copy()
if search_input:
    search_lower = search_input.lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].str.lower().str.contains(search_lower, na=False) |
        filtered_df["Product Name"].str.lower().str.contains(search_lower, na=False)
    ]

st.markdown(f"### Total Products: {df.shape[0]}")
st.dataframe(filtered_df, use_container_width=True)

# --- DOWNLOAD ---
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="openpyxl") as writer:
    filtered_df.to_excel(writer, index=False)
buf.seek(0)
st.download_button(
    "📥 Descargar Excel (Stock)",
    buf,
    file_name="product_stock_analysis(6meses).xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
