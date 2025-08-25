# -*- coding: utf-8 -*-
"""
Weekly ranking report generator (Slack + JSON)
- 월~일 가장 최근 완결 7일 집계, 직전 주와 비교
- 소스: oy_kor, oy_global, amazon_us, qoo10_jp, daiso_kr
- 산출물:
  - slack_{src}.txt
  - weekly_summary_{src}.json
사용:
  python scripts/weekly_report_plus.py --src oy_kor --split --data-dir ./data/daily --min-days 3
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ------------------------------- 기본 설정 -------------------------------

SRC_SPECS = {
    "oy_kor":    {"title": "올리브영 국내 Top100",    "topn": 100},
    "oy_global": {"title": "올리브영 글로벌 Top100",  "topn": 100},
    "amazon_us": {"title": "아마존 US Top100",        "topn": 100},
    "qoo10_jp":  {"title": "큐텐 재팬 뷰티 Top200",   "topn": 200},
    "daiso_kr":  {"title": "다이소몰 뷰티/위생 Top200","topn": 200},
}

# 파일명으로 소스 식별 (느슨한 한글 포함)
FILENAME_HINTS = {
    "oy_kor":    ["올리브영_랭킹", "올리브영 국내", "oy_kor"],
    "oy_global": ["올리브영글로벌", "oy_global"],
    "amazon_us": ["아마존US", "amazon_us"],
    "qoo10_jp":  ["큐텐재팬", "큐텐 재팬", "qoo10_jp"],
    "daiso_kr":  ["다이소몰", "daiso_kr"],
}

SKU_KEY_CANDIDATES = [
    "goodsNo", "productId", "asin", "product_code", "pdNo",
    "item_id", "id", "sku", "url_key"
]

RANK_COL_CAND = ["rank", "순위", "랭킹", "ranking", "Rank"]
BRAND_COL_CAND = ["brand", "브랜드", "Brand"]
NAME_COL_CAND = ["raw_name", "제품명", "상품명", "name", "title"]
URL_COL_CAND  = ["url", "URL", "link", "주소", "링크"]

# -------------------------- 올영픽/PICK/성분 파서 -------------------------

RE_OY_PICK  = re.compile(r"(올영픽|올리브영\s*픽)\b", re.I)
RE_INFL_PK  = re.compile(r"([가-힣A-Za-z0-9.&/_-]+)\s*(픽|Pick)\b", re.I)
EXCLUDE_INFL = {"올영", "올리브영", "월올영", "원픽"}

PAT_MARKETING = {
    "올영픽"   : r"(올영픽|올리브영\s*픽)",
    "특가"     : r"(특가|핫딜|세일|할인)",
    "세트"     : r"(세트|구성|트리오|듀오|패키지|킷\b|키트\b)",
    "기획"     : r"(기획|기획전)",
    "1+1/증정" : r"(1\+1|1\+2|덤|증정|사은품)",
    "한정/NEW" : r"(한정|리미티드|NEW|뉴\b)",
    "쿠폰/딜"  : r"(쿠폰|딜\b|딜가|프로모션|프로모\b)",
}
PAT_MARKETING = {k: re.compile(v, re.I) for k, v in PAT_MARKETING.items()}

DEFAULT_INGRS = [
    "히알루론산","세라마이드","나이아신아마이드","레티놀","펩타이드","콜라겐",
    "비타민C","BHA","AHA","PHA","판테놀","센텔라","마데카소사이드",
]

def load_ingredients_from_file() -> List[str]:
    path = os.path.join("configs", "ingredients.txt")
    if not os.path.exists(path):
        return DEFAULT_INGRS[:]
    words = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            words.append(ln)
    return words or DEFAULT_INGRS[:]

INGR_WORDS = load_ingredients_from_file()

def parse_marketing_and_infl(raw_name: str):
    name = raw_name or ""
    mk = {k: bool(p.search(name)) for k, p in PAT_MARKETING.items()}
    infl = None
    m = RE_INFL_PK.search(name)
    if m:
        cand = re.sub(r"[\[\](),.|·]", "", m.group(1)).strip()
        if cand and cand not in EXCLUDE_INFL and not RE_OY_PICK.search(name):
            infl = cand
    return mk, infl

def extract_ingredients(raw_name: str, ingr_list=None):
    name = raw_name or ""
    ingr_list = ingr_list or INGR_WORDS
    out = []
    for w in ingr_list:
        if re.search(re.escape(w), name, re.I):
            out.append(w)
    return out

# ------------------------------- 유틸 -------------------------------

def find_existing_col(cols: List[str], cands: List[str]) -> Optional[str]:
    for c in cands:
        if c in cols:
            return c
    # 대소문자/공백/한영 혼용 보정
    lowered = {c.lower(): c for c in cols}
    for c in cands:
        key = c.lower()
        if key in lowered:
            return lowered[key]
    return None

def parse_query_param(url: str, key: str) -> Optional[str]:
    if not url:
        return None
    try:
        # 빠른 정규식 파서
        m = re.search(r"[?&]" + re.escape(key) + r"=([^&#]+)", url)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None

def extract_sku(row: Dict, src: str, url_col: Optional[str]) -> str:
    # 1) 명시 필드 우선
    for k in SKU_KEY_CANDIDATES:
        if k in row and pd.notna(row[k]) and str(row[k]).strip():
            return str(row[k]).strip()
    url = str(row.get(url_col, "") or "")
    if src in ("oy_kor",):
        return parse_query_param(url, "goodsNo") or url
    if src in ("oy_global",):
        return parse_query_param(url, "productId") or url
    if src in ("amazon_us",):
        # 아마존은 asin 필드 있거나 URL path에서 추출
        asin = row.get("asin")
        if asin: return str(asin)
        m = re.search(r"/([A-Z0-9]{10})(?:[/?]|$)", url)
        return m.group(1) if m else url
    if src in ("qoo10_jp",):
        return parse_query_param(url, "product_code") or url
    if src in ("daiso_kr",):
        return parse_query_param(url, "pdNo") or url
    return url

def parse_date_from_filename(fn: str) -> Optional[date]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", fn)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None

def last_complete_week(today: Optional[date] = None) -> Tuple[date, date]:
    """
    최근 완결 주(월~일). 오늘이 월~일 중 어디든 상관없이,
    직전 '일요일'까지의 한 주를 반환.
    """
    today = today or date.today()
    # 월=0 ... 일=6
    weekday = today.weekday()
    # 지난 일요일
    last_sunday = today - timedelta(days=(weekday + 1))
    start = last_sunday - timedelta(days=6)  # 월요일
    end = last_sunday
    return start, end

def prev_week_range(start: date, end: date) -> Tuple[date, date]:
    delta = timedelta(days=7)
    return (start - delta, end - delta)

def within(d: date, start: date, end: date) -> bool:
    return start <= d <= end

def ensure_int(x) -> Optional[int]:
    try:
        v = int(float(x))
        return v
    except Exception:
        return None

# ---------------------- 데이터 적재 & 전처리 ----------------------

def guess_src_from_filename(fn: str) -> Optional[str]:
    for src, hints in FILENAME_HINTS.items():
        if any(h in fn for h in hints):
            return src
    return None

def load_daily_files_for_range(src: str, data_dir: str, start: date, end: date) -> List[str]:
    outs = []
    for fn in os.listdir(data_dir):
        full = os.path.join(data_dir, fn)
        if not os.path.isfile(full):
            continue
        d = parse_date_from_filename(fn)
        if not d or not within(d, start, end):
            continue
        guessed = guess_src_from_filename(fn)
        if guessed == src:
            outs.append(full)
    return sorted(outs)

def read_csv_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except Exception:
        try:
            return pd.read_csv(path, encoding="cp949")
        except Exception:
            return pd.read_csv(path, encoding="latin1")

def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 기본 컬럼 존재 보정
    cols = list(df.columns)
    rank_col  = find_existing_col(cols, RANK_COL_CAND)
    brand_col = find_existing_col(cols, BRAND_COL_CAND)
    name_col  = find_existing_col(cols, NAME_COL_CAND)
    url_col   = find_existing_col(cols, URL_COL_CAND)

    # 안전 복사
    out = pd.DataFrame()
    if rank_col:  out["rank"] = df[rank_col]
    if brand_col: out["brand"] = df[brand_col]
    if name_col:  out["raw_name"] = df[name_col]
    if url_col:   out["url"] = df[url_col]

    # 숫자 변환
    if "rank" in out.columns:
        out["rank"] = pd.to_numeric(out["rank"], errors="coerce")

    return out

def load_week_dataframe(src: str, data_dir: str, start: date, end: date, topn: int) -> pd.DataFrame:
    files = load_daily_files_for_range(src, data_dir, start, end)
    frames = []
    for path in files:
        d = parse_date_from_filename(os.path.basename(path))
        df = unify_columns(read_csv_any(path))
        if "rank" not in df.columns:
            continue
        df = df[df["rank"].notnull()]
        df = df.sort_values("rank").head(topn).copy()
        df["date"] = pd.to_datetime(d)
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["rank","brand","raw_name","url","date","date_str"])
    out = pd.concat(frames, ignore_index=True)
    # 빈 brand/raw_name 채움
    out["brand"] = out.get("brand", pd.Series(dtype=str)).fillna("").astype(str)
    out["raw_name"] = out.get("raw_name", pd.Series(dtype=str)).fillna("").astype(str)
    out["url"] = out.get("url", pd.Series(dtype=str)).fillna("").astype(str)
    return out

# --------------------------- 주간 집계 ---------------------------

@dataclass
class ItemStat:
    sku: str
    raw_name: str
    brand: str
    days: int
    avg_rank: float
    min_rank: float
    first_rank: float

def build_week_stats(src: str, df: pd.DataFrame, topn: int) -> Tuple[pd.DataFrame, Dict[str, ItemStat]]:
    if df.empty:
        return df, {}

    # 식별자 생성
    url_col = "url"
    df["sku"] = df.apply(lambda r: extract_sku(r, src, url_col), axis=1)

    # 주간 item 통계
    g = df.groupby("sku")
    stats: Dict[str, ItemStat] = {}
    for sku, sub in g:
        raw = sub["raw_name"].mode().iloc[0] if not sub["raw_name"].isna().all() else ""
        br  = sub["brand"].mode().iloc[0] if not sub["brand"].isna().all() else ""
        days = sub["date_str"].nunique()
        avg_rank = float(sub["rank"].mean())
        min_rank = float(sub["rank"].min())
        first_rank = float(sub.sort_values("date")["rank"].iloc[0])
        stats[sku] = ItemStat(sku, raw, br, days, avg_rank, min_rank, first_rank)
    return df, stats

def compare_prev_week(curr: Dict[str, ItemStat], prev: Dict[str, ItemStat]) -> Dict[str, Optional[float]]:
    """이전 주 평균과의 차이 (prev_avg - curr_avg >0이면 개선)"""
    deltas: Dict[str, Optional[float]] = {}
    for sku, st in curr.items():
        if sku in prev:
            d = prev[sku].avg_rank - st.avg_rank
            deltas[sku] = d
        else:
            deltas[sku] = None  # NEW
    return deltas

def brand_daily_counts(df: pd.DataFrame) -> Dict[str, float]:
    """일별 브랜드 개수 → 일평균"""
    if df.empty:
        return {}
    outs = []
    for d, sub in df.groupby("date_str"):
        counts = Counter([str(b) for b in sub["brand"].fillna("").tolist()])
        outs.append(counts)
    total = Counter()
    for c in outs:
        total.update(c)
    days = max(1, len(outs))
    avg = {k: round(v / days, 1) for k, v in total.items() if k.strip()}
    return dict(sorted(avg.items(), key=lambda x: (-x[1], x[0])))

def inout_daily_average(df: pd.DataFrame, src: str) -> float:
    """일일 교체(IN=OUT) 평균 개수"""
    if df.empty: return 0.0
    url_col = "url"
    df["sku"] = df.apply(lambda r: extract_sku(r, src, url_col), axis=1)
    days = sorted(df["date_str"].unique())
    if len(days) <= 1: return 0.0
    changes = []
    prev_set = set()
    for d in days:
        now_set = set(df[df["date_str"]==d]["sku"])
        if prev_set:
            changes.append(len(now_set - prev_set))  # IN == OUT
        prev_set = now_set
    return round(sum(changes)/len(changes), 1) if changes else 0.0

def hero_flash_lists(stats: Dict[str, ItemStat], prev_stats: Dict[str, ItemStat]) -> Tuple[List[str], List[str]]:
    """
    히어로: 3일 이상 유지 & 지난주엔 없었음
    반짝: 2일 이하 유지
    """
    heroes, flashes = [], []
    for sku, st in stats.items():
        if st.days >= 3 and sku not in prev_stats:
            heroes.append(st.raw_name)
        if st.days <= 2:
            flashes.append(st.raw_name)
    return heroes[:10], flashes[:10]

def kw_summary(df: pd.DataFrame) -> Dict[str, any]:
    """마케팅/인플/성분 요약 (주간 유니크 SKU 기준)"""
    out = {
        "unique": 0,
        "marketing": defaultdict(int),
        "influencers": defaultdict(int),
        "ingredients": defaultdict(int),
    }
    if df.empty: return {"unique": 0, "marketing":{}, "influencers":{}, "ingredients":{}}

    url_col = "url"
    df["sku"] = df.apply(lambda r: extract_sku(r, "oy_kor", url_col), axis=1)  # src 무관: sku만 필요
    uniq = set()
    seen_mk = set()  # (sku, key) 1회만 카운트
    for _, r in df.iterrows():
        sku = r["sku"]
        raw = (r.get("raw_name") or "").strip()
        uniq.add(sku)
        mk, infl = parse_marketing_and_infl(raw)
        for k, v in mk.items():
            if v and (sku, k) not in seen_mk:
                out["marketing"][k] += 1
                seen_mk.add((sku,k))
        if infl:
            out["influencers"][infl] += 1
        for ing in extract_ingredients(raw, INGR_WORDS):
            out["ingredients"][ing] += 1
    out["unique"] = len(uniq)
    # 정렬
    out["marketing"]   = dict(sorted(out["marketing"].items(),   key=lambda x: (-x[1], x[0])))
    out["influencers"] = dict(sorted(out["influencers"].items(), key=lambda x: (-x[1], x[0])))
    out["ingredients"] = dict(sorted(out["ingredients"].items(), key=lambda x: (-x[1], x[0])))
    return out

# --------------------------- 포맷팅(Slack) ---------------------------

def arrow_from_delta(d: Optional[float]) -> str:
    if d is None: return "NEW"
    val = int(round(abs(d)))
    if val == 0: return "유지"
    return f"↑{val}" if d > 0 else f"↓{val}"

def format_top10(stats: Dict[str, ItemStat], deltas: Dict[str, Optional[float]]) -> List[str]:
    # 평균 순위 낮음(좋음) 우선
    items = sorted(stats.values(), key=lambda s: (s.avg_rank, s.min_rank))[:10]
    out = []
    for i, st in enumerate(items, 1):
        line = f"{i}. {st.raw_name} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위) {arrow_from_delta(deltas.get(st.sku))}"
        out.append(line)
    return out

def format_brand_lines(avg_counts: Dict[str, float], limit: int = 15) -> List[str]:
    lines = []
    for k, v in list(avg_counts.items())[:limit]:
        lines.append(f"{k} {v}개/일")
    return lines

def format_kw_block(kw: Dict[str, any]) -> str:
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
        for k, cnt in list(kw["influencers"].items())[:20]:
            lines.append(f"  - {k}: {cnt}개")
    if kw["ingredients"]:
        lines.append("• *성분 키워드*")
        for k, cnt in list(kw["ingredients"].items())[:20]:
            lines.append(f"  - {k}: {cnt}개")
    return "\n".join(lines)

def build_slack_message(src: str,
                        range_str: str,
                        top10_lines: List[str],
                        brand_lines: List[str],
                        inout_avg: float,
                        heroes: List[str],
                        flashes: List[str],
                        kw_text: str) -> str:
    title = SRC_SPECS[src]["title"]
    lines = []
    lines.append(f"📈 *주간 리포트 · {title} ({range_str})*")
    lines.append("")
    # Top10
    lines.append("🏆 *Top10*")
    if top10_lines:
        lines += [f"{ln}" for ln in top10_lines]
    else:
        lines.append("데이터 없음")
    lines.append("")
    # 브랜드
    lines.append("📦 *브랜드 개수(일평균)*")
    if brand_lines:
        lines += [f"{ln}" for ln in brand_lines]
    else:
        lines.append("데이터 없음")
    lines.append("")
    # 인앤아웃
    lines.append("🔁 *인앤아웃(교체)*")
    lines.append(f"- 일평균 {inout_avg}개")
    lines.append("")
    # 신규/반짝
    lines.append("🆕 *신규 히어로(≥3일 유지)*")
    lines.append("없음" if not heroes else "· " + " · ".join(heroes[:8]))
    lines.append("✨ *반짝 아이템(≤2일)*")
    lines.append("없음" if not flashes else "· " + " · ".join(flashes[:8]))
    lines.append("")
    # 키워드 블록
    lines.append(kw_text)
    return "\n".join(lines)

# --------------------------- 메인 파이프라인 ---------------------------

def run_for_source(src: str, data_dir: str, min_days: int = 3) -> Dict[str, any]:
    spec = SRC_SPECS[src]
    topn = spec["topn"]
    # 주차
    start, end = last_complete_week()
    prev_start, prev_end = prev_week_range(start, end)
    range_str = f"{start.strftime('%Y-%m-%d')}-{end.strftime('%Y-%m-%d')}"

    # 데이터 적재
    cur_df = load_week_dataframe(src, data_dir, start, end, topn)
    prev_df = load_week_dataframe(src, data_dir, prev_start, prev_end, topn)

    # 통계
    cur_df, cur_stats = build_week_stats(src, cur_df, topn)
    _, prev_stats = build_week_stats(src, prev_df, topn)
    deltas = compare_prev_week(cur_stats, prev_stats)

    # Top10
    top10_lines = format_top10(cur_stats, deltas)

    # 브랜드 일평균
    brand_avg = brand_daily_counts(cur_df)
    brand_lines = format_brand_lines(brand_avg)

    # 인앤아웃
    inout_avg = inout_daily_average(cur_df, src)

    # 히어로/반짝
    heroes, flashes = hero_flash_lists(cur_stats, prev_stats)

    # 키워드 분석(주간 전체 df 기준)
    kw = kw_summary(cur_df)
    kw_text = format_kw_block(kw)

    # 슬랙 메시지
    slack_text = build_slack_message(
        src, range_str, top10_lines, brand_lines, inout_avg, heroes, flashes, kw_text
    )

    # 파일 저장
    with open(f"slack_{src}.txt", "w", encoding="utf-8") as f:
        f.write(slack_text)

    summary = {
        "range": range_str,
        "top10_lines": top10_lines or ["데이터 없음"],
        "brand_lines": brand_lines or ["데이터 없음"],
        "inout_avg": inout_avg,
        "heroes": heroes,
        "flashes": flashes,
        "kw": kw,  # 원자료(비율은 unique로 계산)
    }
    with open(f"weekly_summary_{src}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary

# --------------------------- CLI ---------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--src", choices=list(SRC_SPECS.keys()) + ["all"], required=True)
    p.add_argument("--data-dir", default="./data/daily")
    p.add_argument("--min-days", type=int, default=3, help="히어로 판정 최소 유지일(기본 3)")
    p.add_argument("--split", action="store_true", help="(호환용) 의미 없음")
    args = p.parse_args()

    if args.src == "all":
        results = {}
        for s in SRC_SPECS.keys():
            print(f"[run] {s}")
            results[s] = run_for_source(s, args.data_dir, args.min_days)
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        res = run_for_source(args.src, args.data_dir, args.min_days)
        print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
