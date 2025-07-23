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

# --- AUTH ---
password = st.text_input("🔐Ingrese la contraseña", type="password")
if password != st.secrets["app_password"]:
    st.stop()

# --- CONFIG ---
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}
MADRID_TZ = pytz.timezone('Europe/Madrid')
BASE_URL = "https://api.holded.com/api/invoicing/v1"

st.set_page_config(page_title="📦 Análisis de Stock", layout="wide")
st.title("📦 Análisis de Stock (Últimos 6 Meses)")

# --- FETCH HOLD PRODUCTS ---
@st.cache_data(ttl=3600)
def fetch_products():
    all_prods = []
    page = 1
    while True:
        resp = requests.get(f"{BASE_URL}/products", headers=HEADERS, params={"page": page})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_prods.extend(batch)
        page += 1
    return pd.DataFrame(all_prods)

# --- FETCH SALESORDERS (Only Status 0 + Time Filter) ---
@st.cache_data(ttl=3600)
def fetch_salesorders_last6months():
    now = datetime.now(MADRID_TZ)
    six_months_ago = now - relativedelta(months=6)
    start_ts = int(six_months_ago.timestamp())
    end_ts = int(now.timestamp())
    url = f"{BASE_URL}/documents/salesorder?starttmp={start_ts}&endtmp={end_ts}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    return df[df["status"] == 0]  # Status 0 = pending orders only

# --- FETCH SHIPPED ITEMS FROM SALESORDERS ---
def fetch_pending_by_sku(salesorders):
    def get_shipped_items(doc_id):
        url = f"{BASE_URL}/documents/salesorder/{doc_id}/shippeditems"
        try:
            res = requests.get(url, headers=HEADERS)
            res.raise_for_status()
            return res.json()
        except:
            return []

    all_items = []
    for _, row in salesorders.iterrows():
        doc_items = get_shipped_items(row["id"])
        for item in doc_items:
            all_items.append({
                "SKU": str(item.get("sku", "")).strip(),
                "Product Name": item.get("name", "").strip(),
                "Units_Pending": item.get("pending", 0)
            })

    df = pd.DataFrame(all_items)
    df = df[df["SKU"].astype(str) != "0"]
    df = df.groupby("SKU", as_index=False).agg({
        "Product Name": lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0],
        "Units_Pending": "sum"
    })
    return df.rename(columns={"Units_Pending": "Stock Reservado"})

# --- FETCH HISTORIC SALES DATA FOR AVERAGES ---
@st.cache_data(ttl=3600)
def fetch_orders_full():
    url = f"{BASE_URL}/documents/salesorder"
    resp = requests.get(url, headers=HEADERS)
    return pd.DataFrame(resp.json())

def expand_order_rows(df):
    rows = []
    for _, row in df.iterrows():
        try:
            products = ast.literal_eval(row["products"]) if isinstance(row["products"], str) else row["products"]
        except:
            continue
        readable_date = datetime.fromtimestamp(row["date"], UTC).astimezone(MADRID_TZ).date()
        for item in products:
            rows.append({
                "SKU": str(item.get("sku", "")).strip(),
                "Product Name": item.get("name", "").strip(),
                "Units": item.get("units", 0),
                "Date": readable_date,
            })
    return pd.DataFrame(rows)

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

# --- LOAD DATA ---
product_df = fetch_products()
orders_df = fetch_orders_full()
sku_units_df = expand_order_rows(orders_df)
sku_units_df = sku_units_df.dropna(subset=["Date", "SKU", "Units"])
sku_units_df = sku_units_df.apply(fix_sku_and_name, axis=1)

# --- FILTER LAST 6 MONTHS ---
today = datetime.now(MADRID_TZ).date()
sku_units_df = sku_units_df[sku_units_df["Date"] >= today - timedelta(days=180)]

# --- WEIGHTED + LINEAR MOVING AVERAGES ---
sku_units_df["Month"] = sku_units_df["Date"].apply(lambda d: d.replace(day=1))
month_bins = [(today - relativedelta(months=i)).replace(day=1) for i in range(6)][::-1]
weights = [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]
month_weight_map = dict(zip(month_bins, weights))

grouped = sku_units_df.groupby(["SKU", "Product Name", "Month"]).agg({"Units": "sum"}).reset_index()
grouped["Weight"] = grouped["Month"].map(month_weight_map)
grouped["Weighted Units"] = grouped["Units"] * grouped["Weight"]

summary_df = grouped.groupby(["SKU", "Product Name"]).agg({
    "Units": "sum",
    "Weighted Units": "sum"
}).reset_index()
summary_df["Media Lineal (Mes)"] = (summary_df["Units"] / 6).round(2)
summary_df["Media Exponencial (Mes)"] = summary_df["Weighted Units"].round(2)
summary_df["Media"] = ((summary_df["Media Lineal (Mes)"] + summary_df["Media Exponencial (Mes)"]) / 2).round(2)

# --- ACTIVE MONTHS ---
sku_units_df["YearMonth"] = sku_units_df["Date"].values.astype("datetime64[M]")
active_months = sku_units_df.groupby(["SKU", "Product Name"])["YearMonth"].nunique().reset_index(name="Active Months")
summary_df = summary_df.merge(active_months, on=["SKU", "Product Name"], how="left")

# --- STOCK REAL ---
product_df["sku"] = product_df["sku"].astype(str)
summary_df["SKU"] = summary_df["SKU"].astype(str)
stock_map = product_df.set_index("sku")["stock"].to_dict()
summary_df["Stock Real"] = summary_df["SKU"].map(stock_map).fillna(0).astype(int)

# --- STOCK RESERVADO + STOCK DISPONIBLE ---
pending_df = fetch_pending_by_sku(fetch_salesorders_last6months())
summary_df = summary_df.merge(pending_df, on="SKU", how="left")
summary_df["Stock Reservado"] = summary_df["Stock Reservado"].fillna(0).astype(int)
summary_df["Stock Disponible"] = summary_df["Stock Real"] - summary_df["Stock Reservado"]

# --- CLEANUP / ORDER ---
summary_df = summary_df.rename(columns={"Units": "Units (Last 6 Months)"})
summary_df = summary_df.sort_values(by="Units (Last 6 Months)", ascending=False)

cols = summary_df.columns.tolist()
cols.insert(cols.index("Units (Last 6 Months)") + 1, cols.pop(cols.index("Stock Real")))
cols.insert(cols.index("Stock Real") + 1, cols.pop(cols.index("Stock Reservado")))
cols.insert(cols.index("Stock Reservado") + 1, cols.pop(cols.index("Stock Disponible")))
summary_df = summary_df[cols]

# --- FILTER UI ---
search_input = st.text_input("🔍 Buscar por SKU o Nombre del Producto")
filtered_df = summary_df.copy()
if search_input:
    search_lower = search_input.lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].str.lower().str.contains(search_lower, na=False) |
        filtered_df["Product Name"].str.lower().str.contains(search_lower, na=False)
    ]

# --- DISPLAY + EXPORT ---
st.markdown(f"### Total Productos: {summary_df.shape[0]}")
st.dataframe(filtered_df, use_container_width=True)

buf1 = io.BytesIO()
with pd.ExcelWriter(buf1, engine="openpyxl") as w:
    filtered_df.to_excel(w, index=False)
buf1.seek(0)
st.download_button(
    "📥 Descargar Excel (Stock)",
    buf1,
    file_name="analisis_stock_6meses.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
