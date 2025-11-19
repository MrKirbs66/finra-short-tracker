import streamlit as st
import polars as pl
import yfinance as yf
import requests
from datetime import datetime, timedelta
import io
st.set_page_config(page_title="Dark Pool Screener", layout="wide")

# === PREVENT ABUSE & STAY UNDER FREE LIMITS ===
st.cache_data(ttl=86400)        # cache everything for 24h
st.cache_resource(ttl=86400)    # same for resources
# =============================================
# 1. Download single FINRA day
# =============================================
@st.cache_data(ttl=3600, show_spinner=False)
def download_finra_date(date_str: str):
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        df = pl.read_csv(
            io.BytesIO(resp.content),
            separator="|",
            has_header=True,
        ).with_columns(pl.lit(datetime.strptime(date_str, "%Y%m%d").date()).alias("Date"))
        return df
    except:
        return None


# =============================================
# 2. Load data – smart lookback
# =============================================
@st.cache_data(ttl=3600, show_spinner="Loading latest FINRA data...")
def load_data(lookback_days: int = 1):
    dfs = []
    today = datetime.now().date()
    days_found = 0
    max_check = 60

    for i in range(max_check):
        dt = today - timedelta(days=i)
        if dt.weekday() >= 5:  # skip weekends
            continue
        date_str = dt.strftime("%Y%m%d")
        df = download_finra_date(date_str)
        if df is not None and not df.is_empty():
            dfs.append(df)
            days_found += 1
            if days_found >= lookback_days:
                break

    if not dfs:
        return pl.DataFrame(), None

    df_all = pl.concat(dfs)

    # === Core calculations – IMPORTANT: BuyVolume must be created FIRST ===
    df_all = df_all.with_columns([
        (pl.col("ShortVolume") + pl.col("ShortExemptVolume")).cast(pl.Int64).alias("BuyVolume"),
        (pl.col("TotalVolume") - (pl.col("ShortVolume") + pl.col("ShortExemptVolume"))).cast(pl.Int64).alias("SellVolume"),
    ]).with_columns([
        pl.when(pl.col("ShortVolume") > 0)
          .then((pl.col("BuyVolume") / pl.col("ShortVolume")).round(3))
          .otherwise(None)
          .alias("BS_Ratio"),
        (pl.col("BuyVolume") / pl.col("TotalVolume")).round(4).alias("DP_Ratio"),
        ((pl.col("BuyVolume") / pl.col("TotalVolume")) * 100).round(1).alias("DP_Index_%"),
    ])

    # Relative volume (10-day avg)
    df_all = df_all.sort(["Symbol", "Date"])
    df_all = df_all.with_columns(
        pl.col("TotalVolume")
        .Rolling_mean(window_size=10, min_periods=1)
        .over("Symbol")
        .alias("Avg10d_Volume")
    ).with_columns(
        (pl.col("TotalVolume") / pl.col("Avg10d_Volume")).round(2).alias("Relative_Volume")
    )

    # Remove garbage low-volume rows
    df_all = df_all.filter(
        (pl.col("TotalVolume") >= 200_000) | (pl.col("BuyVolume") >= 100_000)
    )

    latest_date = df_all["Date"].max()
    return df_all, latest_date


# =============================================
# 3. Market cap (optional)
# =============================================
@st.cache_data(ttl=86400)
def get_market_caps(symbols):
    caps = {}
    for sym in symbols[:70]:
        try:
            info = yf.Ticker(sym).fast_info
            cap = info.get("marketCap")
            caps[sym] = round(cap / 1e9, 2) if cap else 0
        except:
            caps[sym] = 0
    return caps


# =============================================
# 4. MAIN APP
# =============================================
st.set_page_config(page_title="Dark Pool Screener Pro", layout="wide")
st.title("FINRA Dark Pool & Short Volume Screener")
st.caption("Latest day by default • Full history for specific tickers • Real 0.00–1.00 DP ratio")

# Sidebar – Mode selection
st.sidebar.header("View Mode")
mode = st.sidebar.radio("Choose mode", ["Latest Day (All Stocks)", "Specific Tickers (History)"])

df = pl.DataFrame()
latest_date = None

if mode == "Latest Day (All Stocks)":
    df, latest_date = load_data(lookback_days=1)
    st.success(f"Showing **all significant dark pool activity** – {latest_date.strftime('%A, %B %d, %Y')}")

    # Filters for latest day
    min_dp = st.sidebar.slider("Minimum Dark Pool Ratio", 0.0, 1.0, 0.50, 0.05,
                               help="0.50 = 50% of volume was off-exchange (dark pool)")
    min_vol = st.sidebar.slider("Minimum Total Volume", 200_000, 10_000_000, 1_000_000, 100_000)

    filtered = df.filter(
        (pl.col("DP_Ratio") >= min_dp) &
        (pl.col("TotalVolume") >= min_vol)
    )

else:  # Specific tickers + history
    ticker_input = st.sidebar.text_input("Tickers (comma separated)", "GME, AMC, TSLA, NVDA")
    days_back = st.sidebar.number_input("Days of history", 5, 90, 30, step=5)

    tickers = [t.strip().upper() for t in ticker_input.replace(" ", "").split(",") if t.strip()]
    if not tickers:
        st.warning("Please enter at least one ticker.")
        st.stop()

    df, latest_date = load_data(lookback_days=days_back)
    filtered = df.filter(pl.col("Symbol").is_in(tickers))

    st.success(f"Showing **{len(tickers)} tickers** – last **{days_back} trading days** up to **{latest_date.strftime('%B %d, %Y')}**")

# Optional market cap filter
use_cap = st.sidebar.checkbox("Filter by Market Cap", value=False)
if use_cap and not filtered.is_empty():
    min_cap_b = st.sidebar.slider("Min Market Cap ($B)", 0.1, 1000.0, 1.0, 0.5)
    syms = filtered["Symbol"].unique().to_list()
    caps = get_market_caps(syms)
    filtered = filtered.with_columns(
        pl.col("Symbol").map_elements(lambda x: caps.get(x, 0), return_dtype=pl.Float64).alias("MarketCap_B")
    ).filter(pl.col("MarketCap_B") >= min_cap_b)
else:
    filtered = filtered.with_columns(pl.lit(None).alias("MarketCap_B"))

# =============================================
# DISPLAY
# =============================================
if filtered.is_empty():
    st.info("No results match your filters – try loosening them.")
    st.stop()

cols = ["Date", "Symbol", "BuyVolume", "TotalVolume", "DP_Ratio", "DP_Index_%", "BS_Ratio", "Relative_Volume"]
if use_cap:
    cols.append("MarketCap_B")

display_df = (
    filtered.select(cols)
    .sort(["Date", "DP_Ratio"], descending=[True, True])
    .with_columns(pl.col("DP_Ratio").map_elements(lambda x: f"{x:.3f}", return_dtype=pl.Utf8))
)

st.write(f"**{len(display_df):,} dark pool prints found**")
st.dataframe(display_df, use_container_width=True, hide_index=True)

# Chart
if len(display_df) > 1:
    st.subheader("Dark Pool Ratio Trend")
    chart_df = display_df.pivot(
        values="DP_Ratio",
        index="Date",
        columns="Symbol",
        aggregate_function="mean"
    ).sort("Date").fill_null(0)
    st.line_chart(chart_df.set_index("Date"), height=450)
