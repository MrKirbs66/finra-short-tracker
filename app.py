import streamlit as st
import polars as pl
import yfinance as yf
import requests
import os
from datetime import datetime, timedelta
import io

@st.cache_data(ttl=3600)
def download_finra_date(date_str: str):
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
    try:
        resp = requests.get(url)
        if resp.status_code != 200:
            return None
        df = pl.read_csv(
            io.BytesIO(resp.content),
            separator="|",
            has_header=True,
            infer_schema_length=10000
        ).with_columns(pl.lit(datetime.strptime(date_str, "%Y%m%d").date()).alias("Date"))
        return df
    except:
        return None

@st.cache_data(ttl=3600)
def load_historical_data(days_back: int = 30):
    dfs = []
    for i in range(days_back):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        df = download_finra_date(date)
        if df is not None:
            dfs.append(df)
    if not dfs:
        st.warning("No data downloaded yet. Try refreshing.")
        return pl.DataFrame()
    df_all = pl.concat(dfs)
    
    # Calculate custom metrics
    df_all = df_all.with_columns([
        (pl.col("ShortVolume") + pl.col("ShortExemptVolume")).alias("BuyVolume"),
        (pl.col("TotalVolume") - (pl.col("ShortVolume") + pl.col("ShortExemptVolume"))).alias("SellVolume"),
        pl.when(pl.col("ShortVolume") > 0)
          .then((pl.col("BuyVolume") / pl.col("ShortVolume")).round(2))
          .otherwise(None)
          .alias("BS_Ratio"),
        ((pl.col("BuyVolume") / pl.col("TotalVolume")) * 100).round(1).alias("DP_Index")
    ])
    
    # 10-day relative volume
    df_all = df_all.sort(["Symbol", "Date"])
    df_all = df_all.with_columns(
        pl.col("TotalVolume").rolling_mean(window_size="10d", min_periods=1).over("Symbol").alias("Avg10d_Volume")
    ).with_columns(
        (pl.col("TotalVolume") / pl.col("Avg10d_Volume")).round(2).alias("Relative_Volume")
    )
    
    return df_all

@st.cache_data(ttl=86400)
def get_market_caps(symbols: list):
    if not symbols:
        return {}
    tickers = yf.Tickers(" ".join(symbols))
    caps = {}
    for sym in symbols:
        try:
            info = tickers.tickers[sym].info
            caps[sym] = info.get("marketCap", 0) / 1e9  # In billions
        except:
            caps[sym] = 0
    return caps

st.set_page_config(page_title="FINRA Short Volume Tracker", layout="wide")
st.title("🔍 FINRA Reg SHO Short Volume Dashboard")

# Load data
df = load_historical_data(30)  # Last 30 days

if df.is_empty():
    st.info("Downloading initial data... Refresh in a moment.")
else:
    # Sidebar filters
    st.sidebar.header("Filters")
    min_cap_billions = st.sidebar.number_input("Min Market Cap ($B)", 0.0, 100.0, 0.0, 0.1)
    tickers_input = st.sidebar.text_input("Specific Tickers (comma-separated)", "").upper().strip()
    tickers = [t.strip() for t in tickers_input.split(",") if t.strip()] if tickers_input else []
    min_dp = st.sidebar.slider("Min DP Index (%)", 0, 100, 50)
    
    # Apply basic filters
    filtered = df.filter(pl.col("DP_Index") >= min_dp)
    if tickers:
        filtered = filtered.filter(pl.col("Symbol").is_in(tickers))
    
    # Add market caps
    unique_symbols = filtered["Symbol"].unique().to_list()
    caps = get_market_caps(unique_symbols)
    filtered = filtered.with_columns(
        pl.lit(unique_symbols).map_elements(lambda sym: caps.get(sym, 0), return_dtype=pl.Float64).over("Symbol").alias("MarketCap_B")
    ).filter(pl.col("MarketCap_B") >= min_cap_billions)
    
    # Display table
    display_cols = ["Date", "Symbol", "BuyVolume", "ShortVolume", "BS_Ratio", "Relative_Volume", "DP_Index", "TotalVolume", "MarketCap_B"]
    st.subheader("Data Table")
    st.dataframe(
        filtered.select(display_cols).sort("Date", descending=True),
        use_container_width=True,
        hide_index=True
    )
    
    # Quick chart
    if not filtered.is_empty():
        st.subheader("DP Index by Symbol (Last 10 Days)")
        recent = filtered.filter(pl.col("Date") >= (datetime.now().date() - timedelta(days=10)))
        st.bar_chart(recent.pivot(values="DP_Index", index="Date", columns="Symbol", aggregate_function="mean"))
