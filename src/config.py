from datetime import datetime

# ---- Universe ----
TICKERS =[
  "AAPL","MSFT","NVDA","AMZN","GOOGL",
  "META","TSLA","JPM","V","MA",
  "XOM","UNH","HD","PG","LLY",
  "COST","AVGO","PEP","KO","MRK",
]

# ---- Date Range ----
START_DATE = "2010-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# ---- Bars ----
MULTIPLIER = 1
TIMESPAN = "day"

# ---- Modeling Defaults ----
LABEL_HORIZON_DAYS = 5
RETURN_THRESHOLD = 0.01