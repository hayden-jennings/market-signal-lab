import pandas as pd

from tqdm import tqdm

from src.config import (
    TICKERS,
    START_DATE,
    END_DATE,
    MULTIPLIER,
    TIMESPAN,
)

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

            if not df.empty:
                frames.append(df)
        
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    
    if not frames:
        raise RuntimeError("No data pulled.")
    
    final_df = pd.concat(frames).sort_values(["ticker", "datetime"])

    output_path = "data/raw/daily_bars.parquet"
    final_df.to_parquet(output_path, index=False)

    print(f"\nSaved dataset -> {output_path}")
    print(final_df.head())

if __name__ == "__main__":
    main()