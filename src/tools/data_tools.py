from __future__ import annotations

import pandas as pd
from strands import tool

DEFAULT_DATA_PATH = "data/processed/modeling_dataset.parquet"

@tool
def load_modeling_datast(path: str = DEFAULT_DATA_PATH) -> dict:
    """
    Load the modeling dataset parquet and return basic metadata + a small preview.
    Returns dict so it is easy for agents to consume.
    """
    df = pd.read_parquet(path)

    required = {"ticker", "datetime", "label"}
    missing = required - set(df.columns)
    if missing:
        return {"ok": False, "error": f"Missing required columns: {sorted(missing)}"}
    
    df["datetime"] = pd.to_datetime(df["datetime"])

    return {
        "ok": True,
        "path": path,
        "rows": int(len(df)),
        "tickers": sorted(df["ticker"].unique().tolist()),
        "min_date": str(df["datetime"].min()),
        "max_date": str(df["datetime"].max()),
        "label_rate": float(df["label"].mean()),
        "head": df.head(5).to_dict(orient="records"),
    }

@tool
def slice_ticker(df_path: str, ticker: str) -> dict:
    """
    Return all rows for one ticker (sorted by datetime) + basic stats.
    """
    df = pd.read_parquet(df_path)
    df = df[df["ticker"] == ticker].copy()
    if df.empty:
        return {"ok": False, "error": f"No rows for ticker={ticker}"}

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")

    return {
        "ok": True,
        "ticker": ticker,
        "rows": int(len(df)),
        "min_date": str(df["datetime"].min()),
        "max_date": str(df["datetime"].max()),
        "label_rate": float(df["label"].mean()),
        "tail": df.tail(5).to_dict(orient="records"),  # fill
    }