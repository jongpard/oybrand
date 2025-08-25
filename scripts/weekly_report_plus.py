# -*- coding: utf-8 -*-
"""
Weekly ranking report generator (Slack + JSON)
요구사항 반영 사항:
- 집계 구간: '최근 완결 월~일' + 직전 '월~일' 비교 고정
- Top10 정렬: (-유지일, 평균순위, 최저순위) => 반짝 1위 방지
- 등락: (괄호) NEW/유지/↑n/↓n
- 인앤아웃: IN=OUT => '일평균 X.Y개' 한 줄
- 인플루언서: 오직 oy_kor(올리브영 국내)만 집계, '올영픽'과 'PICK' 완전 분리
- 성분: configs/ingredients.txt 동적 로드(없으면 기본 목록)
- 링크: Slack <url|텍스트>, HTML용 anchor 정보 함께 JSON에 저장
- 통계: 'Top100 등극 SKU', 'Top100 유지 평균(일)' 포함
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
from datetime import date, datetime, timedelta
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
    "oy_kor":    ["올리브영_랭킹", "올리브영 국내", "oy_kor"],
    "oy_global": ["올리브영글로벌", "oy_global"],
    "amazon_us": ["아마존US", "amazon_us"],
    "qoo10_jp":  ["큐텐재팬", "큐텐 재팬", "qoo10_jp"],
    "daiso_kr":  ["다이소몰", "daiso_kr"],
}

RANK_COLS  = ["rank", "순위", "랭킹", "ranking", "Rank"]
BRAND_COLS = ["brand", "브랜드", "Brand"]
NAME_COLS  = ["raw_name", "제품명", "상품명", "name", "title"]
URL_COLS   = ["url", "URL", "link", "주소", "링크"]

SKU_KEYS = ["goodsNo", "productId", "asin", "product_code", "pdNo", "sku", "id", "item_id", "url_key"]


# ----------------------- 올영픽 / PICK / 성분 -----------------------
# '올영픽'(프로모션)과 'PICK'(인플루언서)은 별개!
RE_OY_PICK = re.compile(r"(올영픽|올리브영\s*픽)\b", re.I)
RE_INFL_PICK = re.compile(r"([가-힣A-Za-z0-9.&/_-]+)\s*(픽|Pick)\b", re.I)
EXCLUDE_INFL = {"올영", "올리브영", "월올영", "원픽"}  # 인플 후보 제거

# 마케팅 키워드(모두 노출, 병합 금지)
PAT_MARKETING = {
    "올영픽":     r"(올영픽|올리브영\s*픽)",
    "특가":       r"(특가|핫딜|세일|할인)",
    "세트":       r"(세트|패키지|트리오|듀오|세트킷|키트|킷\b)",
    "기획":       r"(기획|기획전)",
    "1+1/증정":   r"(1\+1|1\+2|덤|증정|사은품)",
    "한정/NEW":   r"(한정|리미티드|NEW|뉴\b)",
    "쿠폰/딜":    r"(쿠폰|딜\b|딜가|프로모션|프로모\b)",
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
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            out.append(ln)
    return out or DEFAULT_INGRS[:]

INGR_WORDS = load_ingredients()

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
    out = []
    for w in ingr_list:
        if re.search(re.escape(w), name, re.I):
            out.append(w)
    return out


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

def guess_src_from_filename(fn: str) -> Optional[str]:
    for src, hints in FILENAME_HINTS.items():
        if any(h in fn for h in hints):
            return src
    return None

def parse_date_from_filename(fn: str) -> Optional[date]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", fn)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None

def last_complete_week(today: Optional[date] = None) -> Tuple[date, date]:
    """지난 '일요일' 기준 완결 주(월~일)"""
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
    for enc in ("utf-8", "cp949", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)  # 마지막 시도

def unify_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    out = pd.DataFrame()

    r = first_existing(cols, RANK_COLS)
    b = first_existing(cols, BRAND_COLS)
    n = first_existing(cols, NAME_COLS)
    u = first_existing(cols, URL_COLS)

    if r: out["rank"] = pd.to_numeric(df[r], errors="coerce")
    if b: out["brand"] = df[b].fillna("").astype(str)
    if n: out["raw_name"] = df[n].fillna("").astype(str)
    if u: out["url"] = df[u].fillna("").astype(str)

    return out

def load_files_for_range(src: str, data_dir: str, start: date, end: date) -> List[str]:
    outs = []
    for fn in os.listdir(data_dir):
        full = os.path.join(data_dir, fn)
        if not os.path.isfile(full):
            continue
        d = parse_date_from_filename(fn)
        if not d or not within(d, start, end):
            continue
        if guess_src_from_filename(fn) == src:
            outs.append(full)
    return sorted(outs)

def extract_sku(row: Dict, src: str) -> str:
    # 1) 명시 필드
    for k in SKU_KEYS:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    url = str(row.get("url", "") or "")
    if src == "oy_kor":
        return parse_query(url, "goodsNo") or url
    if src == "oy_global":
        return parse_query(url, "productId") or url
    if src == "amazon_us":
        if row.get("asin"): return str(row["asin"])
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
        if "rank" not in df.columns: 
            continue
        df = df[df["rank"].notnull()].sort_values("rank").head(topn).copy()
        df["date"] = pd.to_datetime(d)
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["rank","brand","raw_name","url","date","date_str"])
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

def build_stats(src: str, df: pd.DataFrame, topn: int) -> Dict[str, ItemStat]:
    stats: Dict[str, ItemStat] = {}
    if df.empty: 
        return stats

    df["sku"] = df.apply(lambda r: extract_sku(r, src), axis=1)
    for sku, sub in df.groupby("sku"):
        raw  = sub["raw_name"].mode().iloc[0] if not sub["raw_name"].isna().all() else ""
        br   = sub["brand"].mode().iloc[0] if not sub["brand"].isna().all() else ""
        url  = sub["url"].mode().iloc[0] if not sub["url"].isna().all() else ""
        days = sub["date_str"].nunique()
        avg  = float(sub["rank"].mean())
        minr = float(sub["rank"].min())
        stats[sku] = ItemStat(sku, raw, br, url, days, avg, minr)
    return stats

def compare_prev(curr: Dict[str, ItemStat], prev: Dict[str, ItemStat]) -> Dict[str, Optional[float]]:
    deltas: Dict[str, Optional[float]] = {}
    for sku, st in curr.items():
        if sku in prev:
            deltas[sku] = prev[sku].avg_rank - st.avg_rank  # +면 개선(↑)
        else:
            deltas[sku] = None
    return deltas

def arrow(d: Optional[float]) -> str:
    if d is None: return "NEW"
    val = int(round(abs(d)))
    if val == 0: return "유지"
    return f"↑{val}" if d > 0 else f"↓{val}"

def top10_for_display(stats: Dict[str, ItemStat], deltas: Dict[str, Optional[float]]) -> Tuple[List[str], List[Dict]]:
    # (-유지일, 평균순위, 최저순위) 정렬
    items = sorted(stats.values(), key=lambda s: (-s.days, s.avg_rank, s.min_rank))[:10]
    slack_lines, html_items = [], []
    for i, st in enumerate(items, 1):
        ar = arrow(deltas.get(st.sku))
        # Slack: <url|텍스트>
        link_txt = f"<{st.url}|{st.raw_name}>" if st.url else st.raw_name
        slack_lines.append(f"{i}. {link_txt} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위) ({ar})")
        html_items.append({
            "idx": i,
            "name": st.raw_name,
            "url": st.url,
            "days": st.days,
            "avg": round(st.avg_rank, 1),
            "arrow": ar,
        })
    return slack_lines, html_items

def brand_daily_avg(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty: return {}
    outs = []
    for d, sub in df.groupby("date_str"):
        cnt = Counter([str(x) for x in sub["brand"].fillna("").tolist() if str(x).strip()])
        outs.append(cnt)
    total = Counter()
    for c in outs:
        total.update(c)
    days = max(1, len(outs))
    avg = {k: round(v / days, 1) for k, v in total.items()}
    return dict(sorted(avg.items(), key=lambda x: (-x[1], x[0])))

def inout_avg_per_day(df: pd.DataFrame, src: str) -> float:
    if df.empty: return 0.0
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

def hero_and_flash(stats: Dict[str, ItemStat], prev_stats: Dict[str, ItemStat]) -> Tuple[List[str], List[str]]:
    heroes, flashes = [], []
    for sku, st in stats.items():
        if st.days >= 3 and sku not in prev_stats:
            heroes.append(st.raw_name)
        if st.days <= 2:
            flashes.append(st.raw_name)
    return heroes[:10], flashes[:10]

def kw_summary(src: str, df: pd.DataFrame) -> Dict[str, any]:
    """주간 유니크 SKU 기준. 인플루언서는 oy_kor만 집계."""
    out = {
        "unique": 0,
        "marketing": defaultdict(int),
        "influencers": defaultdict(int),
        "ingredients": defaultdict(int),
    }
    if df.empty: return {"unique": 0, "marketing":{}, "influencers":{}, "ingredients":{}}

    df = df.copy()
    df["sku"] = df.apply(lambda r: extract_sku(r, src), axis=1)
    uniq = set()
    seen_mk = set()
    for _, r in df.iterrows():
        sku = r["sku"]
        raw = (r.get("raw_name") or "").strip()
        uniq.add(sku)

        mk, infl = parse_marketing_and_infl(raw)
        # 마케팅: 유니크 SKU 기준 1회
        for k, v in mk.items():
            if v and (sku, k) not in seen_mk:
                out["marketing"][k] += 1
                seen_mk.add((sku, k))
        # 인플: 오직 oy_kor일 때만
        if src == "oy_kor" and infl:
            out["influencers"][infl] += 1
        # 성분
        for ing in extract_ingredients(raw, INGR_WORDS):
            out["ingredients"][ing] += 1

    out["unique"] = len(uniq)
    out["marketing"]   = dict(sorted(out["marketing"].items(),   key=lambda x: (-x[1], x[0])))
    out["influencers"] = dict(sorted(out["influencers"].items(), key=lambda x: (-x[1], x[0])))
    out["ingredients"] = dict(sorted(out["ingredients"].items(), key=lambda x: (-x[1], x[0])))
    return out


# --------------------------- 포맷(슬랙/JSON) ---------------------------
def format_kw_for_slack(kw: Dict[str, any]) -> str:
    if kw.get("unique",0) == 0:
        return "데이터 없음"
    lines = []
    lines.append("📊 *주간 키워드 분석*")
    lines.append(f"- 유니크 SKU: {kw['unique']}개")
    if kw["marketing"]:
        lines.append("• *마케팅 키워드*")
        for k, cnt in kw["marketing"].items():
            ratio = round(cnt * 100.0 / max(1, kw["unique"]), 1)
            lines.append(f"  - {k}: {cnt}개 ({ratio}%)")
    if kw["influencers"]:
        lines.append("• *인플루언서*")
        for k, cnt in kw["influencers"].items():
            lines.append(f"  - {k}: {cnt}개")
    if kw["ingredients"]:
        lines.append("• *성분 키워드*")
        for k, cnt in kw["ingredients"].items():
            lines.append(f"  - {k}: {cnt}개")
    return "\n".join(lines)

def format_brand_lines(avg_counts: Dict[str, float], limit: int = 15) -> List[str]:
    return [f"{k} {v}개/일" for k, v in list(avg_counts.items())[:limit]]

def build_slack(src: str, range_str: str,
                top10_lines: List[str],
                brand_lines: List[str],
                inout_avg: float,
                heroes: List[str],
                flashes: List[str],
                kw_text: str,
                unique_cnt: int,
                keep_days_mean: float) -> str:
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
    lines.append("🆕 *신규 히어로(≥3일 유지)*")
    lines.append("없음" if not heroes else "· " + " · ".join(heroes[:8]))
    lines.append("✨ *반짝 아이템(≤2일)*")
    lines.append("없음" if not flashes else "· " + " · ".join(flashes[:8]))
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

    # 주 범위
    start, end = last_complete_week()
    prev_start, prev_end = prev_week_range(start, end)
    range_str = f"{start:%Y-%m-%d}-{end:%Y-%m-%d}"

    # 데이터
    cur_df  = load_week_df(src, data_dir, start, end, topn)
    prev_df = load_week_df(src, data_dir, prev_start, prev_end, topn)

    cur_stats  = build_stats(src, cur_df,  topn)
    prev_stats = build_stats(src, prev_df, topn)

    deltas = compare_prev(cur_stats, prev_stats)

    # Top10
    top10_lines, top10_html_items = top10_for_display(cur_stats, deltas)

    # 브랜드/인앤아웃/히어로
    brand_lines = format_brand_lines(brand_daily_avg(cur_df))
    inout_avg   = inout_avg_per_day(cur_df, src)
    heroes, flashes = hero_and_flash(cur_stats, prev_stats)

    # 키워드
    kw  = kw_summary(src, cur_df)
    kw_text = format_kw_for_slack(kw)

    # 통계
    unique_cnt = len(cur_stats)
    keep_days_mean = 0.0
    if cur_df.shape[0] > 0:
        # SKU별 유지일 평균
        keep_days_mean = sum(st.days for st in cur_stats.values()) / max(1, len(cur_stats))

    # 슬랙 텍스트
    slack_text = build_slack(
        src, range_str, top10_lines, brand_lines, inout_avg, heroes, flashes,
        kw_text, unique_cnt, keep_days_mean
    )
    with open(f"slack_{src}.txt", "w", encoding="utf-8") as f:
        f.write(slack_text)

    # 요약 JSON (HTML 생성을 위해 anchor 정보 포함)
    summary = {
        "range": range_str,
        "title": SRC_SPECS[src]["title"],
        "topn": topn,
        "top10_items": top10_html_items,      # [{name,url,days,avg,arrow}]
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
