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
        resp = requests.get(
            "https://api.holded.com/api/invoicing/v1/products",
            headers=HEADERS,
            params={"page": page}
        )
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
    resp.raise_for_status()
    return pd.DataFrame(resp.json())

# --- FETCH SHIPMENTS (Waybills) ---
@st.cache_data(ttl=3600)
def fetch_shipments():
    url = "https://api.holded.com/api/invoicing/v1/documents/waybill"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())

# --- EXTRACT PRODUCTS FOR PENDING CALC ---
def extract_products(df):
    rows = []
    for _, r in df.iterrows():
        prods = r.get("products")
        if isinstance(prods, str):
            try:
                prods = ast.literal_eval(prods)
            except:
                continue
        if isinstance(prods, list):
            for item in prods:
                rows.append({
                    "SKU": item.get("sku"),
                    "Product Name": item.get("name"),
                    "Units": item.get("units", 0)
                })
    return pd.DataFrame(rows)

# --- SKU & NAME CLEANING (común) ---
def fix_sku_and_name(row):
    if str(row["SKU"]) == "0" or pd.isnull(row["SKU"]):
        name = str(row["Product Name"])
        patterns = [
            r"^(\d+)\s+(.*)",
            r"^SKU\s*(\d+)\s+(.*)",
            r"^(\d+)-\s*(.*)",
            r"^Ref:\s*(\d+)\s+(.*)",
        ]
        for pattern in patterns:
            match = re.match(pattern, name)
            if match:
                row["SKU"] = match.group(1)
                row["Product Name"] = match.group(2)
                break
    return row

# --- CALCULAR STOCK RESERVADO ---
@st.cache_data(ttl=3600)
def fetch_pending_stock():
    # Orders
    orders_df = fetch_orders()
    pedido_prods = extract_products(orders_df).apply(fix_sku_and_name, axis=1)
    pedido_agg = (
        pedido_prods
        .groupby(["SKU", "Product Name"], as_index=False)
        .agg({"Units": "sum"})
        .rename(columns={"Units": "Units_Ordered"})
    )
    # Shipments
    ship_df = fetch_shipments()
    albaran_prods = extract_products(ship_df).apply(fix_sku_and_name, axis=1)
    ship_agg = (
        albaran_prods
        .groupby(["SKU"], as_index=False)
        .agg({"Units": "sum"})
        .rename(columns={"Units": "Units_Shipped"})
    )
    # Merge y calcular pendientes
    pend = pd.merge(
        pedido_agg.drop(columns=["Product Name"], errors="ignore"),
        ship_agg,
        on="SKU",
        how="left"
    )
    pend["Units_Shipped"] = pend["Units_Shipped"].fillna(0).astype(int)
    pend["Stock Reservado"] = (pend["Units_Ordered"] - pend["Units_Shipped"]).clip(lower=0).astype(int)
    return pend[["SKU", "Stock Reservado"]]

# --- FORMATEAR ORDERS PARA HISTÓRICO ---
def expand_order_rows(df):
    rows = []
    for _, row in df.iterrows():
        ts = row["date"]
        try:
            prods = ast.literal_eval(row["products"]) if isinstance(row["products"], str) else row["products"]
        except:
            continue
        fecha = datetime.fromtimestamp(ts, UTC).astimezone(MADRID_TZ).date()
        for item in prods:
            rows.append({
                "SKU": item.get("sku"),
                "Product Name": item.get("name"),
                "Units": item.get("units"),
                "Date": fecha,
            })
    return pd.DataFrame(rows)

# --- START ---
product_df = fetch_products()
order_df = fetch_orders()
sku_units_df = expand_order_rows(order_df).dropna(subset=["Date", "SKU", "Units"])
sku_units_df = sku_units_df.apply(fix_sku_and_name, axis=1)

# --- FILTRAR ÚLTIMOS 6 MESES ---
today = datetime.now(MADRID_TZ).date()
sku_units_df = sku_units_df[sku_units_df["Date"] >= today - timedelta(days=180)]

# --- AGRUPAR POR MES Y CALCULAR MEDIAS ---
sku_units_df["Month"] = sku_units_df["Date"].apply(lambda d: d.replace(day=1))
month_bins = [(today - relativedelta(months=i)).replace(day=1) for i in range(6)][::-1]
weights = [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]
mw = dict(zip(month_bins, weights))

grouped = sku_units_df.groupby(["SKU", "Product Name", "Month"]).agg({"Units": "sum"}).reset_index()
grouped["Weight"] = grouped["Month"].map(mw)
grouped["Weighted Units"] = grouped["Units"] * grouped["Weight"]

summary_df = (
    grouped.groupby(["SKU", "Product Name"])
    .agg({"Units": "sum", "Weighted Units": "sum"})
    .rename(columns={"Units": "Units (Last 6 Months)", "Weighted Units": "Media Exponencial (Mes)"})
    .reset_index()
)
summary_df["Media Lineal (Mes)"] = (summary_df["Units (Last 6 Months)"] / 6).round(2)
summary_df["Media Exponencial (Mes)"] = summary_df["Media Exponencial (Mes)"].round(2)
summary_df["Media"] = ((summary_df["Media Lineal (Mes)"] + summary_df["Media Exponencial (Mes)"]) / 2).round(2)

# --- MESES ACTIVOS ---
sku_units_df["YearMonth"] = sku_units_df["Date"].values.astype("datetime64[M]")
active_months = sku_units_df.groupby(["SKU", "Product Name"])["YearMonth"].nunique().reset_index(name="Active Months")
summary_df = summary_df.merge(active_months, on=["SKU", "Product Name"], how="inner")

# --- MERGE STOCK REAL Y STOCK RESERVADO ---
# Stock real
product_df["sku"] = product_df["sku"].astype(str)
summary_df["SKU"] = summary_df["SKU"].astype(str)
stock_map = product_df.set_index("sku")["stock"].to_dict()
summary_df["Stock Real"] = summary_df["SKU"].map(stock_map).fillna(0).astype(int)
# Stock reservado
pending_df = fetch_pending_stock()
summary_df = summary_df.merge(pending_df, on="SKU", how="left")
summary_df["Stock Reservado"] = summary_df["Stock Reservado"].fillna(0).astype(int)
# Stock disponible
summary_df["Stock Disponible"] = (summary_df["Stock Real"] - summary_df["Stock Reservado"]).astype(int)

# --- REORDENAR COLUMNAS ---
summary_df = summary_df.sort_values(by="Units (Last 6 Months)", ascending=False)
cols = summary_df.columns.tolist()
# Asegurar orden: después de Units -> Stock Real -> Stock Reservado -> Stock Disponible
idx = cols.index("Units (Last 6 Months)")
# mover Stock Real
cols.insert(idx+1, cols.pop(cols.index("Stock Real")))
# mover Stock Reservado y Disponible
cols.insert(idx+2, cols.pop(cols.index("Stock Reservado")))
cols.insert(idx+3, cols.pop(cols.index("Stock Disponible")))
summary_df = summary_df[cols]

# --- FILTRO DE BÚSQUEDA ---
search_input = st.text_input("🔍 Buscar por SKU o Nombre del Producto")
filtered_df = summary_df.copy()
if search_input:
    s = search_input.lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].str.lower().str.contains(s, na=False) |
        filtered_df["Product Name"].str.lower().str.contains(s, na=False)
    ]

# --- DISPLAY & DOWNLOAD ---
st.markdown(f"### Total Products: {summary_df.shape[0]}")
st.dataframe(filtered_df, use_container_width=True)

buf1 = io.BytesIO()
with pd.ExcelWriter(buf1, engine="openpyxl") as w:
    filtered_df.to_excel(w, index=False)
buf1.seek(0)
st.download_button(
    "📥 Download Excel (Stock)",
    buf1,
    file_name="product_stock_analysis(6 meses).xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
