from datetime import datetime, timedelta

# ---- Universe ----
TICKERS =[ "OSS","ONDS","AMD","CRSP","NOW" ]

# ---- Date Range ----
END_DATE = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=365*2 - 7)).strftime("%Y-%m-%d")

# ---- Bars ----
MULTIPLIER = 1
TIMESPAN = "day"

# ---- Modeling Defaults ----
LABEL_HORIZON_DAYS = 5
RETURN_THRESHOLD = 0.01