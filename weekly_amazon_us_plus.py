# weekly_amazon_us_plus.py
import os, json
from agg_plus import run_weekly
DATA_DIR = os.getenv("DATA_DIR", "./data/daily")
print(json.dumps({"amazon_us": run_weekly(DATA_DIR, "amazon_us", min_days=3)}, ensure_ascii=False, indent=2))
