import requests
import polars as pl
from datetime import datetime
import io

def download_today():
    date_str = datetime.now().strftime("%Y%m%d")
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
    resp = requests.get(url)
    if resp.status_code == 200:
        df = pl.read_csv(io.BytesIO(resp.content), separator="|", has_header=True)
        print(f"Downloaded {len(df)} rows for {date_str}")
        return df
    else:
        print(f"No data for {date_str}")
        return None

if __name__ == "__main__":
    download_today()
