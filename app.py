import streamlit as st
import polars as pl
import yfinance as yf
import requests
from datetime import datetime, timedelta
import io

# --------------------------------------------------------------
# 1. Download single day
# --------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Downloading FINRA data...")
def download_finra_date(date_str: str):
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        df = pl.read_csv(
            io.BytesIO(resp.content),
            separator="|",
            has_header=True,
            try_parse_dates=False,
        )
        # Add proper Date column
        df = df.with_columns(
            pl.lit(datetime.strptime(date_str, "%Y%m%d").date()).alias("Date")
        )
        return df
    except:
        return None


# --------------------------------------------------------------
# 2. Load last N trading days
# --------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Building dataset...")
def load_historical_data(days_back: int = 35):
    dfs = []
    for i in range(days_back):
        dt = datetime.now().date() - timedelta(days=i)
        # Skip weekends
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%Y%m%d")
        df = download_finra_date(date_str)
        if df is not None and not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    # Combine all days
    df_all = pl.concat(dfs)

    # ------------------- All calculations in correct order -------------------
    df_all = df_all.with_columns([
        # 1. Buy Volume = Short + Exempt
        (pl.col("ShortVolume") + pl.col("ShortExemptVolume")).alias("BuyVolume"),

        # 2. Sell Volume
        (pl.col("TotalVolume") - (pl.col("ShortVolume") + pl.col("ShortExemptVolume"))).alias("SellVolume"),
    ]).with_columns([
        # 3. B/S Ratio (Buy / Short)
        pl.when(pl.col("ShortVolume") > 0)
          .then((pl.col("BuyVolume") / pl.col("ShortVolume")).round(2))
          .otherwise(None)
          .alias("BS_Ratio"),

        # 4. DP Index = % of volume that went through dark pools / off-exchange
        ((pl.col("BuyVolume") / pl.col("TotalVolume")) * 100).round(1).alias("DP_Index"),
    ])

    # 5. 10-day average volume + Relative Volume
    df_all = df_all.sort(["Symbol", "Date"])
    df_all = df_all.with_columns(
        pl.col("TotalVolume")
        .rolling_mean(window_size=10, min_periods=1)
        .over("Symbol")
        .alias("Avg10d_Volume")
    ).with_columns(
        (pl.col("TotalVolume") / pl.col("Avg10d_Volume")).round(2).alias("Relative_Volume")
    )

    return df_all


# --------------------------------------------------------------
# 3. Market cap lookup
# --------------------------------------------------------------
@st.cache_data(ttl=86400)
def get_market_caps(symbols):
    if not symbols:
        return {}
    caps = {}
    tickers = yf.Tickers(" ".join(symbols[:100]))  # limit to avoid rate-limit
    for sym in symbols:
        try:
            info = tickers.tickers[sym].info
            cap = info.get("marketCap")
            caps[sym] = round(cap / 1e9, 2) if cap else 0  # in billions
        except:
            caps[sym] = 0
    return caps


# --------------------------------------------------------------
# 4. Streamlit UI
# --------------------------------------------------------------
st.set_page_config(page_title="FINRA Short Volume Tracker", layout="wide")
st.title("FINRA Reg SHO Short Volume Dashboard")
st.caption("Real-time dark pool & short volume tracker • Updates daily")

df = load_historical_data(35)

if df.is_empty():
    st.warning("No data yet — initial download in progress. Refresh in 30–60 seconds.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
min_cap = st.sidebar.slider("Min Market Cap ($B)", 0.0, 100.0, 0.5, 0.1)
tickers_input = st.sidebar.text_input("Specific Tickers (comma-separated)", "").upper().strip()
target_tickers = [t.strip() for t in tickers_input.split(",") if t.strip()]
min_dp = st.sidebar.slider("Minimum DP Index (%)", 0, 100, 50)

# Apply filters
filtered = df.filter(pl.col("DP_Index") >= min_dp)

if target_tickers:
    filtered = filtered.filter(pl.col("Symbol").is_in(target_tickers))

# Add market cap
unique_syms = filtered["Symbol"].unique().to_list()
caps = get_market_caps(unique_syms)
filtered = filtered.with_columns(
    pl.col("Symbol").map_elements(lambda x: caps.get(x, 0), return_dtype=pl.Float64).alias("MarketCap_B")
).filter(pl.col("MarketCap_B") >= min_cap)

# Display
display_cols = [
    "Date", "Symbol", "BuyVolume", "ShortVolume", "BS_Ratio",
    "Relative_Volume", "DP_Index", "TotalVolume", "MarketCap_B"
]

st.dataframe(
    filtered.select(display_cols).sort("Date", descending=True),
    use_container_width=True,
    hide_index=True
)

if not filtered.is_empty():
    st.subheader("DP Index Trend (Last 10 Days)")
    chart_data = filtered.filter(pl.col("Date") >= datetime.now().date() - timedelta(days=10))
    if len(chart_data) > 0:
        pivot = chart_data.pivot(
            values="DP_Index", index="Date", columns="Symbol", aggregate_function="mean"
        ).fill_null(0)
        st.line_chart(pivot.drop("Date"), height=400)
