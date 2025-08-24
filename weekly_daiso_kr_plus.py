# weekly_daiso_kr_plus.py
import os, json
from agg_plus import run_weekly
DATA_DIR = os.getenv("DATA_DIR", "./data/daily")
print(json.dumps({"daiso_kr": run_weekly(DATA_DIR, "daiso_kr", min_days=3)}, ensure_ascii=False, indent=2))
