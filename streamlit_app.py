import streamlit as st
import pandas as pd
import numpy as np
import requests
import ast
import io
import re
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
import pytz

# --- AUTH ---
password = st.text_input("🔐Ingrese la contraseña", type="password")
if password != st.secrets["app_password"]:
    st.stop()

# --- CONFIG ---
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}
BASE_URL = "https://api.holded.com/api/invoicing/v1"
MADRID_TZ = pytz.timezone('Europe/Madrid')

st.set_page_config(page_title="📦 Análisis de Stock", layout="wide")
st.title("📦 Análisis de Stock (Últimos 6 Meses)")

# --- TIME RANGE ---
now = datetime.now(MADRID_TZ)
six_months_ago = now - relativedelta(months=6)
start_ts = int(six_months_ago.timestamp())
end_ts = int(now.timestamp())

# --- FETCH PRODUCTS ---
@st.cache_data(ttl=3600)
def fetch_products():
    all_prods = []
    page = 1
    while True:
        resp = requests.get(f"{BASE_URL}/products", headers=HEADERS, params={"page": page})
        resp.raise_for_status()
        data = resp.json()
        batch = data if isinstance(data, list) else data.get("items", [])
        if not batch:
            break
        all_prods.extend(batch)
        page += 1
    return pd.DataFrame(all_prods)

# --- FETCH SALES ORDERS (FOR ACTIVE MONTHS) ---
@st.cache_data(ttl=3600)
def fetch_orders():
    url = f"{BASE_URL}/documents/salesorder?starttmp={start_ts}&endtmp={end_ts}"
    resp = requests.get(url, headers=HEADERS)
    return pd.DataFrame(resp.json())

# --- FETCH PENDING DATA ---
@st.cache_data(ttl=3600)
def fetch_pending_units():
    orders_df = fetch_orders()
    orders_df = orders_df[orders_df["docNumber"].str.startswith("SO", na=False)]

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
        except Exception as e:
            print(f"❌ Error fetching {doc_number}: {e}")
            return []

    all_rows = []
    for _, row in orders_df.iterrows():
        all_rows.extend(get_shipped_items(row["id"], row["docNumber"]))

    df = pd.DataFrame(all_rows)
    df = df[df["SKU"].astype(str) != "0"]
    df = df.apply(fix_sku_and_name, axis=1)

    agg = (
        df.groupby("SKU", as_index=False)
        .agg({
            "Product Name": lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0],
            "Units_Pending": "sum"
        })
    )
    return agg

# --- EXPAND ORDER PRODUCTS ---
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

# --- FIX SKU + NAME ---
def fix_sku_and_name(row):
    sku = str(row.get("SKU", "")).strip()
    name = str(row.get("Product Name", "")).strip()
    if sku == "0" or sku.lower() in ["none", "nan"]:
        patterns = [
            r"^(\d+)\s+(.*)", r"^SKU\s*(\d+)\s+(.*)", r"^(\d+)-\s*(.*)", r"^Ref:\s*(\d+)\s+(.*)"
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

# --- LOAD DATA ---
product_df = fetch_products()
order_df = fetch_orders()
pending_df = fetch_pending_units()

sku_units_df = expand_order_rows(order_df)
sku_units_df = sku_units_df.dropna(subset=["Date", "SKU", "Units"])
sku_units_df = sku_units_df.apply(fix_sku_and_name, axis=1)

# --- FILTER LAST 6 MONTHS ---
today = now.date()
sku_units_df = sku_units_df[sku_units_df["Date"] >= today - timedelta(days=180)]

# --- MONTHLY AGGREGATION ---
sku_units_df["Month"] = sku_units_df["Date"].apply(lambda d: d.replace(day=1))
month_bins = [(today - relativedelta(months=i)).replace(day=1) for i in range(6)][::-1]
weights = [0.125, 0.125, 0.125, 0.125, 0.25, 0.25]
month_weight_map = dict(zip(month_bins, weights))

grouped = sku_units_df.groupby(["SKU", "Product Name", "Month"]).agg({"Units": "sum"}).reset_index()
grouped["Weight"] = grouped["Month"].map(month_weight_map)
grouped["Weighted Units"] = grouped["Units"] * grouped["Weight"]

# --- SUMMARY CALCULATIONS ---
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

# --- MERGE PENDING UNITS ---
pending_df["SKU"] = pending_df["SKU"].astype(str)
summary_df = summary_df.merge(pending_df[["SKU", "Units_Pending"]], on="SKU", how="left").fillna({"Units_Pending": 0})
summary_df = summary_df.rename(columns={"Units_Pending": "Stock Reservado"})
summary_df["Stock Reservado"] = summary_df["Stock Reservado"].astype(int)

# --- Stock Disponible ---
summary_df["Stock Disponible"] = summary_df["Stock Real"] - summary_df["Stock Reservado"]

# --- FINAL FORMATTING ---
summary_df = summary_df.rename(columns={"Units": "Units (Last 6 Months)"})
summary_df["Units (Last 6 Months)"] = summary_df["Stock Reservado"]  # Replace with Pending
summary_df = summary_df.sort_values(by="Units (Last 6 Months)", ascending=False)

# --- REORDER COLUMNS ---
cols = summary_df.columns.tolist()
cols.insert(cols.index("Units (Last 6 Months)") + 1, cols.pop(cols.index("Stock Real")))
cols.insert(cols.index("Stock Real") + 1, cols.pop(cols.index("Stock Reservado")))
cols.insert(cols.index("Stock Reservado") + 1, cols.pop(cols.index("Stock Disponible")))
summary_df = summary_df[cols]

# --- FILTER FIELD ---
search_input = st.text_input("🔍 Buscar por SKU o Nombre del Producto")
filtered_df = summary_df.copy()

if search_input:
    search_lower = search_input.lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].str.lower().str.contains(search_lower, na=False) |
        filtered_df["Product Name"].str.lower().str.contains(search_lower, na=False)
    ]

# --- DISPLAY ---
st.markdown(f"### Total Productos: {filtered_df.shape[0]}")
st.dataframe(filtered_df, use_container_width=True)

# --- DOWNLOAD ---
buf1 = io.BytesIO()
with pd.ExcelWriter(buf1, engine="openpyxl") as writer:
    filtered_df.to_excel(writer, index=False)
buf1.seek(0)
st.download_button(
    "📥 Descargar Excel",
    buf1,
    file_name="analisis_stock_6_meses.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
