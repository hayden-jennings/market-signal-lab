import pandas as pd

from src.config import LABEL_HORIZON_DAYS, RETURN_THRESHOLD

RAW_PATH = "data/raw/daily_bars.parquet"
OUT_PATH = "data/processed/modeling_dataset.parquet"

def main():
    # Load raw data
    df = pd.read_parquet(RAW_PATH)

    # Sort
    df = df.sort_values(["ticker", "datetime"]).reset_index(drop=True)

    # Basic returns (past)
    df["ret_1d"] = df.groupby("ticker")["close"].pct_change(1)
    df["ret_5d"] = df.groupby("ticker")["close"].pct_change(5)

    # Rolling volatility: std dev of daily returns over 20 days
    df["vol_20d"] = (
        df.groupby("ticker")["ret_1d"]
        .rolling(window=20)
        .std()
        .reset_index(level=0, drop=True)
    )

    # Volume z-score over 20 days: (vol - mean) / std
    vol_mean = (
        df.groupby("ticker")["volume"]
        .rolling(window=20)
        .mean()
        .reset_index(level=0, drop=True)
    )
    vol_std = (
        df.groupby("ticker")["volume"]
        .rolling(window=20)
        .std()
        .reset_index(level=0, drop=True)
    )
    df["vol_z_20d"] = (df["volume"] - vol_mean) / vol_std

    # Forward return over horizon (next N trading days)
    # forward_return = close[t+N] / close[t] - 1
    df["fwd_ret"] = (
        df.groupby("ticker")["close"]
        .shift(-LABEL_HORIZON_DAYS) 
        / df["close"] - 1
    )

    # Label
    df["label"] = (df["fwd_ret"] > RETURN_THRESHOLD).astype(int)

    # Keep only needed columns + drop NaNs created by rolling/shift
    feature_cols = ["ret_1d", "ret_5d", "vol_20d", "vol_z_20d"]
    keep_cols = ["ticker", "datetime", "close"] + feature_cols + ["fwd_ret", "label"]
    df = df[keep_cols].dropna()

    # Save processed dataset
    df.to_parquet(OUT_PATH, index=False)

    # Print sanity checks
    print("Saved ->", OUT_PATH)
    print("Shape:", df.shape)
    print("Unique tickers:", sorted(df["ticker"].unique()))
    print("Label mean (pct positive):", df["label"].mean())
    print(df.head(5))

if __name__ == "__main__":
    main()