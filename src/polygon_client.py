import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = os.getenv("MARKET_DATA_BASE_URL", "https://api.polygon.io")

def get_aggregates(
    ticker: str,
    multiplier: int,
    timespan: str,
    start_date: str,
    end_date: str,
):
    """
    Fetch OHLCV data for a given ticker and time range from the Polygon Aggregates API.
    Pagination is handled automatically to retrieve all available data.
    """

    if not API_KEY:
        raise ValueError("POLYGON_API_KEY not found.")

    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,  # Max limit per request
        "apiKey": API_KEY,
    }

    all_results = []

    while True:
        max_retries = 6
        backoff = 1.0
        
        for _ in range(max_retries):
            response = requests.get(url, params=params)
        
            # Rate limit handling
            if response.status_code == 429:
                print(f"Rate limit hit. Retrying in {backoff:.1f} seconds...")
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff
                continue
                
            response.raise_for_status()
            data = response.json()
            break

        else:
            #exhausted retries
            return pd.DataFrame()  # Return empty DataFrame on failure


        results = data.get("results", [])
        all_results.extend(results)

        next_url = data.get("next_url")
        if not next_url:
            break

        url = next_url
        params = {"apiKey": API_KEY}  # Only need to pass the API key for subsequent requests

        time.sleep(0.2)  # Respect API rate limits

    if not all_results:
        print(f"No data found for {ticker} in the specified date range.")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_results)

    df["ticker"] = ticker
    df["datetime"] = pd.to_datetime(df["t"], unit="ms")

    df = df.rename(
        columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
        }
    )

    return df[["ticker", "datetime", "open", "high", "low", "close", "volume"]]