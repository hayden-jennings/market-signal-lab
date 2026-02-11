import time
import pandas as pd

from tqdm import tqdm

from src.config import (
    TICKERS,
    START_DATE,
    END_DATE,
    MULTIPLIER,
    TIMESPAN,
)

print(f"Pulling data for {TICKERS} tickers from {START_DATE} to {END_DATE}...")
from src.polygon_client import get_aggregates

def main():
    frames = []

    for ticker in tqdm(TICKERS):
        try:
            df = get_aggregates(
                ticker=ticker,
                multiplier=MULTIPLIER,
                timespan=TIMESPAN,
                start_date=START_DATE,
                end_date=END_DATE,
            )

            required_cols = {"ticker", "datetime", "open", "high", "low", "close", "volume"}
            if not df.empty and required_cols.issubset(df.columns):
                frames.append(df)
            else:
                print(f"Skipping {ticker}: empty or missing columns (cols={list(df.columns)})")
        
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

        time.sleep(12.5) # Only 5 requests per minute allowed
    
    if not frames:
        raise RuntimeError("No data pulled.")
    
    final_df = pd.concat(frames).sort_values(["ticker", "datetime"])

    output_path = "data/raw/daily_bars.parquet"
    final_df.to_parquet(output_path, index=False)

    print(f"\nSaved dataset -> {output_path}")
    print("Unique tickers:", sorted(final_df["ticker"].unique()))
    print(final_df.groupby("ticker").size().rename("rows_per_ticker"))
    print(final_df.sort_values(["ticker","datetime"]).groupby("ticker").head(2))

if __name__ == "__main__":
    main()