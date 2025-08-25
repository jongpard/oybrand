# -*- coding: utf-8 -*-
"""
Weekly ranking report generator (Slack + JSON)

- 주간 구간: 가장 최근 '완결 월~일' + 직전 주
- Top10 정렬: (-유지일, 평균순위, 최저순위)
- 등락 표기: (NEW/유지/↑n/↓n)
- 인앤아웃: IN=OUT → '일평균 X.Y개'
- 인플루언서: 올리브영 국내만, '올영픽'과 'PICK(콜라보)' 완전 분리
- 성분 키워드: configs/ingredients.txt 동적 로드(없으면 기본 목록)
- 어떤 CSV라도 결손 컬럼이 있어도 죽지 않도록 방어 처리

출력:
  - slack_{src}.txt
  - weekly_summary_{src}.json

사용:
  python scripts/weekly_report_plus.py --src all --data-dir ./data/daily
"""

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ----------------------------- 소스 스펙 -----------------------------
SRC_SPECS = {
    "oy_kor":    {"title": "올리브영 국내 Top100",     "topn": 100},
    "oy_global": {"title": "올리브영 글로벌 Top100",   "topn": 100},
    "amazon_us": {"title": "아마존 US Top100",         "topn": 100},
    "qoo10_jp":  {"title": "큐텐 재팬 뷰티 Top200",    "topn": 200},
    "daiso_kr":  {"title": "다이소몰 뷰티/위생 Top200", "topn": 200},
}

FILENAME_HINTS = {
    "oy_kor":    ["올리브영_랭킹", "올리브영국내", "올리브영 국내", "oy_kor"],
    "oy_global": ["올리브영글로벌", "oy_global"],
    "amazon_us": ["아마존US", "amazon_us", "amazon"],
    "qoo10_jp":  ["큐텐재팬", "큐텐 재팬", "qoo10_jp", "qoo10"],
    "daiso_kr":  ["다이소몰", "daiso_kr", "daiso"],
}

# 파일마다 표기가 제각각이어서 후보를 넓게 잡음
RANK_COLS  = ["rank", "순위", "랭킹", "ranking", "Rank", "순번"]
BRAND_COLS = ["brand", "브랜드", "Brand", "상표", "제조사/브랜드"]
NAME_COLS  = [
    "raw_name", "제품명", "상품명", "name", "title", "displayName", "product_name",
    "item_name", "상품명(옵션)", "상품", "품목명", "모델명"
]
URL_COLS   = ["url", "URL", "link", "주소", "링크", "상품URL", "page_url", "detail_url"]

SKU_KEYS = ["goodsNo", "productId", "asin", "product_code", "pdNo", "sku", "id", "item_id", "url_key"]


# ----------------------- 올영픽 / PICK / 성분 -----------------------
RE_OY_PICK = re.compile(r"(올영픽|올리브영\s*픽)\b", re.I)
RE_INFL_PICK = re.compile(r"([가-힣A-Za-z0-9.&/_-]+)\s*(픽|Pick)\b", re.I)
EXCLUDE_INFL = {"올영", "올리브영", "월올영", "원픽"}

PAT_MARKETING = {
    "올영픽": r"(올영픽|올리브영\s*픽)",
    "PICK":   r"\bPICK\b",                           # 콜라보 PICK 존재 자체를 마케팅 키워드로 집계
    "특가":   r"(특가|핫딜|세일|할인)",
    "세트":   r"(세트|패키지|트리오|듀오|세트킷|키트|킷\b)",
    "기획":   r"(기획|기획전)",
    "1+1":    r"(?:^|\s)1\+1(?:\s|$)",               # ← 1+1만 별도 집계
    "증정":   r"(증정|사은품)",                      # ← 증정/사은품 별도 집계
    "한정":   r"(한정|리미티드)",
    "NEW":    r"\bNEW\b|(?<!리)뉴\b",
}
PAT_MARKETING = {k: re.compile(v, re.I) for k, v in PAT_MARKETING.items()}

DEFAULT_INGRS = [
    "히알루론산","세라마이드","나이아신아마이드","레티놀","펩타이드","콜라겐",
    "비타민C","BHA","AHA","PHA","판테놀","센텔라","마데카소사이드",
]

def load_ingredients() -> List[str]:
    path = os.path.join("configs", "ingredients.txt")
    if not os.path.exists(path):
        return DEFAULT_INGRS[:]
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            out.append(ln)
    return out or DEFAULT_INGRS[:]

INGR_WORDS = load_ingredients()


# ------------------------------ 유틸 ------------------------------
def first_existing(cols, candidates) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    lower = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def parse_query(url: str, key: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"[?&]" + re.escape(key) + r"=([^&#]+)", url)
    return m.group(1) if m else None

def normalize_key(s: str) -> str:
    return re.sub(r"\s+", "", str(s).lower())

def guess_src_from_filename(fn: str) -> Optional[str]:
    key = normalize_key(fn)
    for src, hints in FILENAME_HINTS.items():
        for h in hints:
            if normalize_key(h) in key:
                return src
    return None

def parse_date_from_filename(fn: str) -> Optional[date]:
    # 2025-08-23 / 2025_08_23 / 2025.08.23 허용
    m = re.search(r"(20\d{2})[-_\.](\d{2})[-_\.](\d{2})", fn)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None

def last_complete_week(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    weekday = today.weekday()  # 월=0 ... 일=6
    last_sun = today - timedelta(days=weekday + 1)
    start = last_sun - timedelta(days=6)
    return start, last_sun

def prev_week_range(start: date, end: date) -> Tuple[date, date]:
    return (start - timedelta(days=7), end - timedelta(days=7))

def within(d: date, start: date, end: date) -> bool:
    return start <= d <= end


# ------------------------- 데이터 적재/정제 -------------------------
def read_csv_any(path: str) -> pd.DataFrame:
    # 서로 다른 인코딩을 최대한 흡수
    for enc in ("utf-8", "cp949", "utf-8-sig", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def unify_cols(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼 이름이 제각각인 CSV를 표준 컬럼으로 맞춘다.
    - 반드시 rank, brand, raw_name, url 컬럼을 가진 DataFrame을 반환
      (없으면 빈 문자열/NaN으로 채워서라도 생성)
    """
    cols = list(df.columns)
    out = pd.DataFrame()

    # 순위
    r = first_existing(cols, RANK_COLS)
    if r:
        out["rank"] = pd.to_numeric(df[r], errors="coerce")
    else:
        # rank가 없으면 이 파일은 무시될 것(상위에서 체크)
        out["rank"] = pd.Series(dtype="float64")

    # 브랜드/제품명/URL
    b = first_existing(cols, BRAND_COLS)
    n = first_existing(cols, NAME_COLS)
    u = first_existing(cols, URL_COLS)

    out["brand"]    = df[b].fillna("").astype(str) if b else ""
    out["raw_name"] = df[n].fillna("").astype(str) if n else ""
    out["url"]      = df[u].fillna("").astype(str) if u else ""

    # 혹시라도 전부 비면 최소한 공백 문자열 형태로 보장
    for col in ("brand", "raw_name", "url"):
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str)

    return out

def load_files_for_range(src: str, data_dir: str, start: date, end: date) -> List[str]:
    outs = []
    if not os.path.isdir(data_dir):
        print(f"[scan] {src}: data_dir not found: {data_dir}")
        return []
    for fn in os.listdir(data_dir):
        full = os.path.join(data_dir, fn)
        if not os.path.isfile(full):
            continue
        d = parse_date_from_filename(fn)
        if not d or not within(d, start, end):
            continue
        if guess_src_from_filename(fn) == src:
            outs.append(full)
    print(f"[scan] {src}: range {start}~{end} -> {len(outs)} file(s) in {data_dir}")
    return sorted(outs)

def extract_sku(row: Dict, src: str) -> str:
    for k in SKU_KEYS:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    url = str(row.get("url", "") or "")
    if src == "oy_kor":
        return parse_query(url, "goodsNo") or url
    if src == "oy_global":
        return parse_query(url, "productId") or url
    if src == "amazon_us":
        if row.get("asin"):
            return str(row["asin"])
        m = re.search(r"/([A-Z0-9]{10})(?:[/?]|$)", url)
        return m.group(1) if m else url
    if src == "qoo10_jp":
        return parse_query(url, "product_code") or url
    if src == "daiso_kr":
        return parse_query(url, "pdNo") or url
    return url

def load_week_df(src: str, data_dir: str, start: date, end: date, topn: int) -> pd.DataFrame:
    files = load_files_for_range(src, data_dir, start, end)
    frames = []
    for p in files:
        d = parse_date_from_filename(os.path.basename(p))
        df = unify_cols(read_csv_any(p))
        # 순위 없으면 스킵
        if "rank" not in df.columns or df["rank"].isna().all():
            continue
        df = df[df["rank"].notnull()].sort_values("rank").head(topn).copy()
        df["date_str"] = (d or start).strftime("%Y-%m-%d")
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["rank","brand","raw_name","url","date_str"])
    return pd.concat(frames, ignore_index=True)


# --------------------------- 주간 통계 ---------------------------
@dataclass
class ItemStat:
    sku: str
    raw_name: str
    brand: str
    url: str
    days: int
    avg_rank: float
    min_rank: float

def _safe_mode(series: Optional[pd.Series]) -> str:
    if series is None:
        return ""
    s = series.dropna().astype(str)
    if s.empty:
        return ""
    try:
        return s.mode().iloc[0]
    except Exception:
        return s.iloc[0]

def build_stats(src: str, df: pd.DataFrame, topn: int) -> Dict[str, ItemStat]:
    stats: Dict[str, ItemStat] = {}
    if df.empty:
        return stats
    # 필수 컬럼 보장
    for col in ("raw_name", "brand", "url"):
        if col not in df.columns:
            df[col] = ""

    df["sku"] = df.apply(lambda r: extract_sku(r, src), axis=1)
    for sku, sub in df.groupby("sku"):
        raw  = _safe_mode(sub.get("raw_name"))
        br   = _safe_mode(sub.get("brand"))
        url  = _safe_mode(sub.get("url"))
        days = sub["date_str"].nunique() if "date_str" in sub else 0
        avg  = float(pd.to_numeric(sub["rank"], errors="coerce").mean())
        minr = float(pd.to_numeric(sub["rank"], errors="coerce").min())
        stats[sku] = ItemStat(sku, raw, br, url, days, avg, minr)
    return stats

def compare_prev(curr: Dict[str, ItemStat], prev: Dict[str, ItemStat]) -> Dict[str, Optional[float]]:
    deltas: Dict[str, Optional[float]] = {}
    for sku, st in curr.items():
        if sku in prev:
            deltas[sku] = prev[sku].avg_rank - st.avg_rank
        else:
            deltas[sku] = None
    return deltas

def arrow(d: Optional[float]) -> str:
    if d is None: return "NEW"
    val = int(round(abs(d)))
    if val == 0: return "유지"
    return f"↑{val}" if d > 0 else f"↓{val}"

def top10_for_display(stats: Dict[str, ItemStat], deltas: Dict[str, Optional[float]]) -> Tuple[List[str], List[Dict]]:
    items = sorted(stats.values(), key=lambda s: (-s.days, s.avg_rank, s.min_rank))[:10]
    slack_lines, html_items = [], []
    for i, st in enumerate(items, 1):
        ar = arrow(deltas.get(st.sku))
        link_txt = f"<{st.url}|{st.raw_name}>" if st.url else st.raw_name
        slack_lines.append(f"{i}. {link_txt} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위) ({ar})")
        html_items.append({"idx": i, "name": st.raw_name, "url": st.url, "days": st.days, "avg": round(st.avg_rank, 1), "arrow": ar})
    return slack_lines, html_items

def brand_daily_avg(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty: return {}
    outs = []
    for d, sub in df.groupby("date_str"):
        cnt = Counter([str(x) for x in sub.get("brand", pd.Series([], dtype=str)).fillna("").tolist() if str(x).strip()])
        outs.append(cnt)
    total = Counter()
    for c in outs:
        total.update(c)
    days = max(1, len(outs))
    avg = {k: round(v / days, 1) for k, v in total.items()}
    return dict(sorted(avg.items(), key=lambda x: (-x[1], x[0])))

def inout_avg_per_day(df: pd.DataFrame, src: str) -> float:
    if df.empty: return 0.0
    if "date_str" not in df.columns: return 0.0
    df = df.copy()
    df["sku"] = df.apply(lambda r: extract_sku(r, src), axis=1)
    days = sorted(df["date_str"].unique())
    if len(days) <= 1: return 0.0
    changes = []
    prev_set = set()
    for d in days:
        now = set(df[df["date_str"]==d]["sku"])
        if prev_set:
            changes.append(len(now - prev_set))  # IN == OUT
        prev_set = now
    return round(sum(changes)/len(changes), 1) if changes else 0.0

def hero_and_flash(stats: Dict[str, ItemStat], prev_stats: Dict[str, ItemStat]):
    # 히어로: 이번 주 3일 이상 유지 & 지난 주엔 없던 상품
    heroes = [st for sku, st in stats.items() if st.days >= 3 and sku not in prev_stats]
    # 반짝: 이번 주 2일 이하
    flashes = [st for _, st in stats.items() if st.days <= 2]

    heroes.sort(key=lambda s: (-s.days, s.avg_rank, s.min_rank))
    flashes.sort(key=lambda s: (s.days, s.avg_rank, s.min_rank))
    return heroes[:10], flashes[:10]

def parse_marketing_and_infl(raw_name: str) -> Tuple[Dict[str, bool], Optional[str]]:
    name = raw_name or ""
    mk = {k: bool(p.search(name)) for k, p in PAT_MARKETING.items()}
    infl = None
    m = RE_INFL_PICK.search(name)
    if m:
        cand = re.sub(r"[\[\](),.|·]", "", m.group(1)).strip()
        if cand and cand not in EXCLUDE_INFL and not RE_OY_PICK.search(name):
            infl = cand
    return mk, infl

def extract_ingredients(raw_name: str, ingr_list=None) -> List[str]:
    name = raw_name or ""
    ingr_list = ingr_list or INGR_WORDS
    out: List[str] = []
    for w in ingr_list:
        if re.search(re.escape(w), name, re.I):
            out.append(w)
    return out

def kw_summary(src: str, df: pd.DataFrame) -> Dict[str, any]:
    out = {
        "unique": 0,
        "marketing": defaultdict(int),
        "influencers": defaultdict(int),
        "ingredients": defaultdict(int),
    }
    if df.empty: return {"unique": 0, "marketing":{}, "influencers":{}, "ingredients":{}}

    df = df.copy()
    if "raw_name" not in df.columns: df["raw_name"] = ""
    df["sku"] = df.apply(lambda r: extract_sku(r, src), axis=1)

    uniq = set()
    seen_mk = set()
    for _, r in df.iterrows():
        sku = r["sku"]
        raw = (r.get("raw_name") or "").strip()
        uniq.add(sku)

        mk, infl = parse_marketing_and_infl(raw)
        for k, v in mk.items():
            if v and (sku, k) not in seen_mk:
                out["marketing"][k] += 1
                seen_mk.add((sku, k))

        if src == "oy_kor" and infl:
            out["influencers"][infl] += 1

        for ing in extract_ingredients(raw, INGR_WORDS):
            out["ingredients"][ing] += 1

    out["unique"] = len(uniq)
    out["marketing"]   = dict(sorted(out["marketing"].items(),   key=lambda x: (-x[1], x[0])))
    out["influencers"] = dict(sorted(out["influencers"].items(), key=lambda x: (-x[1], x[0])))
    out["ingredients"] = dict(sorted(out["ingredients"].items(), key=lambda x: (-x[1], x[0])))
    return out


# --------------------------- 포맷(슬랙/JSON) ---------------------------
def format_kw_for_slack(kw: Dict[str, any]) -> str:
    if kw.get("unique", 0) == 0:
        return "데이터 없음"

    def pct(cnt: int) -> float:
        return round(cnt * 100.0 / max(1, kw["unique"]), 1)

    lines = []
    lines.append("📊 *주간 키워드 분석*")
    lines.append(f"- 유니크 SKU: {kw['unique']}개")

    if kw["marketing"]:
        mk_parts = [f"{k} {v}개({pct(v)}%)" for k, v in kw["marketing"].items()]
        lines.append("• *마케팅 키워드* " + " · ".join(mk_parts))  # ← 가로 나열

    if kw["influencers"]:
        infl_parts = [f"{k} {v}개" for k, v in kw["influencers"].items()]
        lines.append("• *인플루언서* " + " · ".join(infl_parts))   # ← 가로 나열

    if kw["ingredients"]:
        ing_parts = [f"{k} {v}개" for k, v in kw["ingredients"].items()]
        lines.append("• *성분 키워드* " + " · ".join(ing_parts))   # ← 가로 나열

    return "\n".join(lines)

def format_brand_lines(avg_counts: Dict[str, float], limit: int = 15) -> List[str]:
    return [f"{k} {v}개/일" for k, v in list(avg_counts.items())[:limit]]

# 교체: build_slack(...) 내부의 히어로/반짝 출력 부분
def build_slack(src, range_str, top10_lines, brand_lines, inout_avg,
                heroes, flashes, kw_text, unique_cnt, keep_days_mean):
    title = SRC_SPECS[src]["title"]
    lines = []
    lines.append(f"📈 *주간 리포트 · {title} ({range_str})*")
    lines.append("")
    lines.append("🏆 *Top10*")
    lines += (top10_lines or ["데이터 없음"])
    lines.append("")
    lines.append("📦 *브랜드 개수(일평균)*")
    lines += (brand_lines or ["데이터 없음"])
    lines.append("")
    lines.append("🔁 *인앤아웃(교체)*")
    lines.append(f"- 일평균 {inout_avg}개")
    lines.append("")

    # ▼ 히어로: 세로 + 링크
    lines.append("🆕 *신규 히어로(≥3일 유지)*")
    if not heroes:
        lines.append("없음")
    else:
        for st in heroes:
            nm = f"<{st.url}|{st.raw_name}>" if st.url else st.raw_name
            lines.append(f"- {nm} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위)")

    # ▼ 반짝: 세로 + 링크
    lines.append("✨ *반짝 아이템(≤2일)*")
    if not flashes:
        lines.append("없음")
    else:
        for st in flashes:
            nm = f"<{st.url}|{st.raw_name}>" if st.url else st.raw_name
            lines.append(f"- {nm} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위)")

    lines.append("")
    lines.append("📌 *통계*")
    lines.append(f"- Top{SRC_SPECS[src]['topn']} 등극 SKU : {unique_cnt}개")
    lines.append(f"- Top {SRC_SPECS[src]['topn']} 유지 평균 : {keep_days_mean:.1f}일")
    lines.append("")
    lines.append(kw_text)
    return "\n".join(lines)

# ------------------------------ 메인 ------------------------------
def run_for_source(src: str, data_dir: str) -> Dict[str, any]:
    spec = SRC_SPECS[src]
    topn = spec["topn"]

    start, end = last_complete_week()
    prev_start, prev_end = prev_week_range(start, end)
    range_str = f"{start:%Y-%m-%d}-{end:%Y-%m-%d}"

    cur_df  = load_week_df(src, data_dir, start, end, topn)
    prev_df = load_week_df(src, data_dir, prev_start, prev_end, topn)

    cur_stats  = build_stats(src, cur_df,  topn)
    prev_stats = build_stats(src, prev_df, topn)

    deltas = compare_prev(cur_stats, prev_stats)

    top10_lines, top10_html_items = top10_for_display(cur_stats, deltas)
    brand_lines = format_brand_lines(brand_daily_avg(cur_df))
    inout_avg   = inout_avg_per_day(cur_df, src)
    heroes, flashes = hero_and_flash(cur_stats, prev_stats)

    kw  = kw_summary(src, cur_df)
    kw_text = format_kw_for_slack(kw)

    unique_cnt = len(cur_stats)
    keep_days_mean = 0.0
    if cur_df.shape[0] > 0 and len(cur_stats) > 0:
        keep_days_mean = sum(st.days for st in cur_stats.values()) / max(1, len(cur_stats))

    slack_text = build_slack(
        src, range_str, top10_lines, brand_lines, inout_avg, heroes, flashes,
        kw_text, unique_cnt, round(keep_days_mean, 1)
    )
    with open(f"slack_{src}.txt", "w", encoding="utf-8") as f:
        f.write(slack_text)

    summary = {
        "range": range_str,
        "title": SRC_SPECS[src]["title"],
        "topn": topn,
        "top10_items": top10_html_items,
        "brand_lines": brand_lines or ["데이터 없음"],
        "inout_avg": inout_avg,
        "heroes": heroes,
        "flashes": flashes,
        "kw": kw,
        "unique_cnt": unique_cnt,
        "keep_days_mean": round(keep_days_mean, 1),
    }
    with open(f"weekly_summary_{src}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", choices=list(SRC_SPECS.keys()) + ["all"], required=True)
    ap.add_argument("--data-dir", default="./data/daily")
    args = ap.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)

    if args.src == "all":
        results = {}
        for s in SRC_SPECS.keys():
            print(f"[run] {s}")
            results[s] = run_for_source(s, args.data_dir)
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        res = run_for_source(args.src, args.data_dir)
        print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
