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

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    
filter_by_so = st.selectbox(
    "📄 Filtrar solo pedidos con docNumber de 'SO'? (excluye pedidos de Wix)",
    options=["Sí", "No"],
    index=0
)


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
@st.cache_data(ttl=600000)
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
@st.cache_data(ttl=600000)
def fetch_salesorders():
    url = f"{BASE_URL}/documents/salesorder?starttmp={start_ts}&endtmp={end_ts}"
    resp = requests.get(url, headers=HEADERS)
    return pd.DataFrame(resp.json())

# --- FETCH SHIPPED ITEMS ---
@st.cache_data(ttl=600000)  # 🟡 NEW — cache shipped item results by doc_id/doc_number
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

# --- WRAPPED FULL PIPELINE IN CACHE --- 🟡 NEW
@st.cache_data(ttl=600000)
def process_data():
    product_df = fetch_products()
    raw_sales_df = fetch_salesorders()
    if filter_by_so == "Sí":
        sales_df = raw_sales_df[raw_sales_df["docNumber"].str.startswith("SO", na=False)]
    else:
        sales_df = raw_sales_df

    shipped_rows = []
    for _, row in sales_df.iterrows():
        shipped_rows += get_shipped_items(row["id"], row["docNumber"])

    df = pd.DataFrame(shipped_rows)
    df = df.apply(fix_sku_and_name, axis=1)
    df = df[~df["SKU"].isin(["", "0", None, np.nan])]
    df = df[~df["Product Name"].str.lower().eq("shipping")]

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
    df["Stock Disponible"] = df["Stock Real"] - df["Stock Reservado"]

    return df

# --- CALL CACHED PIPELINE --- 🟡 NEW
df = process_data()

# --- Accurate Active Months --- 🟡 NEW
@st.cache_data(ttl=600000)
def fetch_full_salesorders():
    url = f"{BASE_URL}/documents/salesorder?starttmp={start_ts}&endtmp={end_ts}"
    resp = requests.get(url, headers=HEADERS)
    return pd.DataFrame(resp.json())

def extract_sku_dates(order_df):
    rows = []
    for _, row in order_df.iterrows():
        try:
            products = ast.literal_eval(row["products"]) if isinstance(row["products"], str) else row["products"]
        except:
            continue
        readable_date = datetime.fromtimestamp(row["date"], UTC).astimezone(MADRID_TZ).date()
        for item in products:
            rows.append({
                "SKU": str(item.get("sku", "")).strip(),
                "Date": readable_date
            })
    return pd.DataFrame(rows)

orders_raw = fetch_full_salesorders()
sku_date_df = extract_sku_dates(orders_raw)
sku_date_df["Month"] = sku_date_df["Date"].apply(lambda d: d.replace(day=1))
six_months_ago_date = (now - relativedelta(months=6)).replace(day=1).date()
sku_date_df = sku_date_df[sku_date_df["Month"] >= six_months_ago_date]  # 🟡 NEW

active_months_df = sku_date_df.groupby("SKU")["Month"].nunique().reset_index(name="Active Months")
df = df.drop(columns=["Active Months"], errors="ignore")
df = df.merge(active_months_df, on="SKU", how="left")
df["Active Months"] = df["Active Months"].fillna(0).clip(upper=6).astype(int)  # 🟡 NEW



# --- Linear Average ---
df["Media Lineal (Mes)"] = (df["Units (Last 6 Months)"] / 6).round(2)

# --- Real Weighted Monthly Sales ---
@st.cache_data(ttl=600000)
def get_weighted_units_by_sku():
    full_orders = fetch_full_salesorders()
    rows = []
    for _, row in full_orders.iterrows():
        try:
            products = ast.literal_eval(row["products"]) if isinstance(row["products"], str) else row["products"]
        except:
            continue
        readable_date = datetime.fromtimestamp(row["date"], UTC).astimezone(MADRID_TZ).date()
        for item in products:
            rows.append({
                "SKU": str(item.get("sku", "")).strip(),
                "Date": readable_date,
                "Units": item.get("units", 0)
            })
    df_monthly = pd.DataFrame(rows)
    df_monthly["Month"] = df_monthly["Date"].apply(lambda d: d.replace(day=1))
    
    # Last 6 months only
    month_bins = [(now - relativedelta(months=i)).replace(day=1).date() for i in range(6)][::-1]
    df_monthly = df_monthly[df_monthly["Month"].isin(month_bins)]
    
    # Apply weights
    weight_map = dict(zip(month_bins, [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]))
    df_monthly["Weight"] = df_monthly["Month"].map(weight_map)
    df_monthly["Weighted"] = df_monthly["Units"] * df_monthly["Weight"]

    weighted_summary = df_monthly.groupby("SKU")["Weighted"].sum().reset_index(name="Media Exponencial (Mes)")
    return weighted_summary

# Merge real weighted average
weighted_df = get_weighted_units_by_sku()
df = df.merge(weighted_df, on="SKU", how="left")
df["Media Exponencial (Mes)"] = df["Media Exponencial (Mes)"].fillna(0).round(2)

df["Media"] = ((df["Media Lineal (Mes)"] + df["Media Exponencial (Mes)"]) / 2).round(2)


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
    "📥 Descargar Excel",
    buf,
    file_name="analisis_stock_6meses.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
