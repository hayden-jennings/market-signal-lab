import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DATA_PATH = "data/processed/modeling_dataset.parquet"

def main():

    df = pd.read_parquet(DATA_PATH)

    # ---- Featured selection ----
    feature_cols = [
        "ret_1d",
        "ret_5d",
        "vol_20d",
        "vol_z_20d",
    ]

    X = df[feature_cols]
    y = df["label"]

    # ---- Time split ----
    split_idx = int(len(df) * 0.7)

    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]

    # ---- Model ----
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])

    model.fit(X_train, y_train)

    # ---- Predictions ----
    probs = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, probs)
    print("ROC AUC:", auc)

    preds = (probs > 0.5).astype(int)
    print(classification_report(y_test, preds))

    print("Prob range:", float(probs.min()), "to", float(probs.max()))
    print("Prob mean:", float(probs.mean()))

if __name__ == "__main__":
    main()