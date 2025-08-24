# weekly_qoo10_jp_plus.py
import os, json
from agg_plus import run_weekly
DATA_DIR = os.getenv("DATA_DIR", "./data/daily")
print(json.dumps({"qoo10_jp": run_weekly(DATA_DIR, "qoo10_jp", min_days=3)}, ensure_ascii=False, indent=2))
