# weekly_oy_kor_plus.py
import os, json
from agg_plus import run_weekly
DATA_DIR = os.getenv("DATA_DIR", "./data/daily")
print(json.dumps({"oy_kor": run_weekly(DATA_DIR, "oy_kor", min_days=3)}, ensure_ascii=False, indent=2))
