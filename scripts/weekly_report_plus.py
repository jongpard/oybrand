# -*- coding: utf-8 -*-
"""
weekly_report_plus.py
- 일일 CSV(소스별) → 주간 집계 → weekly_summary.json 생성
- Top10은 7일 유지일(내림차순) 우선 + 평균순위(오름차순)로 선별
- 키워드(제품형태/효능/마케팅/성분/인플루언서) 인라인 집계의 원천 데이터 생성

사용법:
  python scripts/weekly_report_plus.py --src all --data-dir ./data/daily
"""

from __future__ import annotations
import argparse
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# -----------------------------------------------------
# 소스 메타
# -----------------------------------------------------
SRC_META = {
    "oy_kor": {
        "title": "올리브영 국내 Top100",
        "topn": 100,
        "glob": "*올리브영_랭킹_*.csv",
        "influencer_on": True,
    },
    "oy_global": {
        "title": "올리브영 글로벌 Top100",
        "topn": 100,
        "glob": "*올리브영글로벌_랭킹_*.csv",
        "influencer_on": False,
    },
    "amazon_us": {
        "title": "아마존 US Top100",
        "topn": 100,
        "glob": "*아마존US_뷰티_랭킹_*.csv",
        "influencer_on": False,
    },
    "qoo10_jp": {
        "title": "큐텐 재팬 뷰티 Top200",
        "topn": 200,
        "glob": "*큐텐재팬_뷰티_랭킹_*.csv",
        "influencer_on": False,
    },
    "daiso_kr": {
        "title": "다이소몰 뷰티/위생 Top200",
        "topn": 200,
        "glob": "*다이소몰_뷰티위생_일간_*.csv",
        "influencer_on": False,
    },
}

SRC_ORDER = ["oy_kor", "oy_global", "amazon_us", "qoo10_jp", "daiso_kr"]

# -----------------------------------------------------
# 공통 유틸
# -----------------------------------------------------
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

POSS_BRAND = ["brand", "브랜드", "brand_name"]
POSS_NAME = ["product_name", "name", "상품명", "title"]
POSS_RAW = ["raw_name", "raw", "제품명_raw"]
POSS_URL = ["url", "URL", "링크", "link", "상품URL", "상품url"]
POSS_CODE = ["sku", "product_code", "ASIN", "asin", "code", "상품코드"]
POSS_RANK = ["rank", "랭킹", "순위", "ranking"]

def _read_csv_any(p: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    # 마지막 시도
    return pd.read_csv(p, engine="python")

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # 소문자 normalize
    lower_map = {str(c).lower(): c for c in df.columns}
    for c in candidates:
        lc = c.lower()
        if lc in lower_map:
            return lower_map[lc]
    return None

def _to_num(val) -> float | None:
    try:
        f = float(str(val).replace(",", "").strip())
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None

def _extract_date_from_name(name: str) -> date | None:
    m = DATE_RE.search(name)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d").date()

def _last_full_week(dates: List[date]) -> Tuple[date, date]:
    """파일 날짜 목록에서 마지막 날짜 기준으로 그 주 '월~일'"""
    if not dates:
        today = date.today()
        # 오늘이 속한 '월~일'
        monday = today - timedelta(days=(today.weekday()))
        sunday = monday + timedelta(days=6)
        return monday, sunday
    dmax = max(dates)
    monday = dmax - timedelta(days=dmax.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday

# -----------------------------------------------------
# 키워드 추출(간단 규칙)
# -----------------------------------------------------
TOKEN = re.compile(r"[A-Za-z0-9가-힣\+\#]+")

PRODUCT_TYPE = ["앰플","세럼","크림","로션","토너","스킨","패드","마스크","팩","클렌저","선크림","선에센스","클렌징","밤","에센스","미스트","파우더","틴트","립","폼","로션","젤"]
BENEFITS = ["보습","진정","톤업","미백","주름","브라이트닝","각질","모공","탄력","지성","민감","복합","저자극","수분","쿨링"]
# '마케팅'은 모두 분리(묶지 않음)
MARKETING = ["1+1","증정","한정","NEW","쿠폰","딜","특가","세트","기획","픽","올영픽","PICK"]
INGREDIENTS = ["세라마이드","히알루론","비타민","판테놀","센텔라","마데카소사이드","니아신아마이드","PHA","AHA","BHA","레티놀","콜라겐","녹차","프로폴리스"]

def extract_keywords(raw_series: pd.Series, src: str) -> Dict[str, List[str]]:
    prod, ben, mkt, ing, infl = [], [], [], [], []

    for val in raw_series.dropna().astype(str).tolist():
        s = val.strip()
        # 토큰 단위 탐색
        toks = TOKEN.findall(s)

        # 제품형태/효능/성분
        for t in PRODUCT_TYPE:
            if t in s:
                prod.append(t)
        for t in BENEFITS:
            if t in s:
                ben.append(t)
        for t in INGREDIENTS:
            if t in s:
                ing.append(t)

        # 마케팅 (분리)
        for t in MARKETING:
            if re.search(rf"\b{re.escape(t)}\b", s, flags=re.IGNORECASE) or (t in s):
                mkt.append(t)

        # 인플루언서(국내만 집계)
        if src == "oy_kor":
            # pick 앞 단어를 '인플루언서'로 취급 (예: [이하빈PICK])
            # 아주 단순 패턴
            m = re.search(r"([가-힣A-Za-z]+)\s*PICK", s, flags=re.IGNORECASE)
            if m:
                infl.append(m.group(1))

    def top_unique(lst):
        if not lst:
            return []
        c = Counter(lst)
        # 너무 많으면 상위 20개만
        return [k for k, _ in c.most_common(20)]

    return {
        "product_type": top_unique(prod),
        "benefits": top_unique(ben),
        "marketing": top_unique(mkt),
        "ingredients": top_unique(ing),
        "influencers": top_unique(infl),
    }

# -----------------------------------------------------
# 로딩 & 정규화
# -----------------------------------------------------
def load_source_df(data_dir: Path, src: str) -> pd.DataFrame:
    meta = SRC_META[src]
    files = sorted((data_dir.glob(meta["glob"])))
    rows = []
    for p in files:
        try:
            df = _read_csv_any(p)
        except Exception:
            continue
        fdate = _extract_date_from_name(p.name)
        if fdate is None:
            # csv 안에서 date 컬럼이 있을 수 있음
            if "date" in df.columns:
                try:
                    fdate = pd.to_datetime(df["date"].iloc[0]).date()
                except Exception:
                    fdate = None
        df["__file_date__"] = fdate
        df["__file__"] = p.name
        rows.append(df)
    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)

    # 컬럼 매핑
    brand_col = _pick_col(df, POSS_BRAND)
    name_col = _pick_col(df, POSS_NAME)
    raw_col = _pick_col(df, POSS_RAW) or name_col
    url_col = _pick_col(df, POSS_URL)
    code_col = _pick_col(df, POSS_CODE)
    rank_col = _pick_col(df, POSS_RANK)
    date_col = "date" if "date" in df.columns else "__file_date__"

    out = pd.DataFrame()
    if brand_col: out["brand"] = df[brand_col]
    else: out["brand"] = None

    out["name"] = df[name_col] if name_col else None
    out["raw"] = df[raw_col] if raw_col else out["name"]
    out["url"] = df[url_col] if url_col else None
    out["code"] = df[code_col] if code_col else None
    out["rank"] = df[rank_col] if rank_col else None
    out["date"] = df[date_col]

    # 타입 정리
    out["rank"] = out["rank"].apply(_to_num)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["src"] = src

    # 키 (sku)
    # code > url > raw 우선순위
    def build_key(r):
        return r["code"] or r["url"] or r["raw"]
    out["sku"] = out.apply(build_key, axis=1)

    return out.dropna(subset=["date", "rank", "sku"])

# -----------------------------------------------------
# 집계
# -----------------------------------------------------
def weekly_window(df_all: pd.DataFrame) -> Tuple[date, date]:
    dlist = df_all["date"].dropna().unique().tolist()
    dlist = [pd.to_datetime(x).date() for x in dlist]
    s, e = _last_full_week(dlist)
    return s, e

def aggregate_source(df: pd.DataFrame, src: str, start: date, end: date) -> Dict:
    meta = SRC_META[src]
    topn = meta["topn"]

    # (A) 날짜 정규화 — 비교 타입 혼재 방지 (핵심 패치)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    start = pd.to_datetime(start).date()
    end = pd.to_datetime(end).date()
    mask = (df["date"] >= start) & (df["date"] <= end)
    df = df.loc[mask].copy()

    # Top N만 사용
    df = df[df["rank"].apply(lambda x: (x is not None) and (x <= topn))].copy()
    if df.empty:
        return {
            "range": f"{start}~{end}",
            "title": meta["title"],
            "topn": topn,
            "top10_items": [],
            "brand_lines": ["데이터 없음"],
            "inout_avg": 0.0,
            "heroes": [],
            "flashes": [],
            "kw": {"product_type":[], "benefits":[], "marketing":[], "ingredients":[], "influencers":[]},
            "unique_cnt": 0,
            "keep_days_mean": 0.0,
        }

    # 일 단위 보정
    all_days = set(start + timedelta(days=i) for i in range(7))

    # 제품 단위 집계
    recs = []
    for sku, grp in df.groupby("sku", as_index=False):
        # (B) Series 접근은 dict 방식만 사용 — 속성 접근 금지 (핵심 패치)
        # -> 아래에서 row.get(...) 만 사용
        days = set(grp["date"].dropna().tolist())
        keep_days = len(days & all_days)
        avg_rank = grp["rank"].astype(float).mean()

        # 대표 raw/url/brand
        any_row = grp.iloc[0]
        raw = any_row.get("raw") if isinstance(any_row, pd.Series) else None
        name = any_row.get("name") if isinstance(any_row, pd.Series) else None
        url  = any_row.get("url") if isinstance(any_row, pd.Series) else None
        brand = any_row.get("brand") if isinstance(any_row, pd.Series) else None

        recs.append({
            "sku": sku,
            "raw": raw or name or "",
            "url": url,
            "brand": brand,
            "keep_days": int(keep_days),
            "avg_rank": float(avg_rank) if not math.isnan(avg_rank) else None,
        })

    # Top10 선발: 유지일 desc, 평균순위 asc
    recs.sort(key=lambda x: (-(x["keep_days"]), x["avg_rank"] if x["avg_rank"] is not None else 9999))

    top10 = recs[:10]
    heroes = [r for r in recs if r["keep_days"] >= 3][:10]
    flashes = [r for r in recs if r["keep_days"] <= 2][:10]

    # 브랜드 라인 (브랜드별/일평균 개수)
    bcnt = Counter([r["brand"] for r in recs if r.get("brand")])
    brand_lines = []
    for b, c in bcnt.most_common(15):
        brand_lines.append(f"{b} {c/7:.1f}개/일")

    # 키워드
    kw = extract_keywords(df.get("raw") if "raw" in df.columns else df.get("name"), src)

    # 통계
    unique_cnt = len(recs)
    keep_mean = sum(r["keep_days"] for r in recs) / max(1, unique_cnt)
    inout_avg = round(unique_cnt / 7.0, 1)

    # delta/변동 방향(옵션) — 평균순위가 낮을수록 ↑
    for r in recs:
        r["delta"] = 0  # 실제 비교 데이터가 없으므로 0으로 둠

    return {
        "range": f"{start}~{end}",
        "title": meta["title"],
        "topn": topn,
        "top10_items": top10,
        "brand_lines": brand_lines or ["데이터 없음"],
        "inout_avg": inout_avg,
        "heroes": heroes,
        "flashes": flashes,
        "kw": kw,
        "unique_cnt": unique_cnt,
        "keep_days_mean": round(keep_mean, 1) if keep_mean else 0.0,
    }

# -----------------------------------------------------
# 메인
# -----------------------------------------------------
def run_for_source(src: str, data_dir: Path) -> Dict:
    df = load_source_df(data_dir, src)
    if df.empty:
        today = date.today()
        s = today - timedelta(days=today.weekday())
        e = s + timedelta(days=6)
        return {
            "range": f"{s}~{e}",
            "title": SRC_META[src]["title"],
            "topn": SRC_META[src]["topn"],
            "top10_items": [],
            "brand_lines": ["데이터 없음"],
            "inout_avg": 0.0,
            "heroes": [],
            "flashes": [],
            "kw": {"product_type":[], "benefits":[], "marketing":[], "ingredients":[], "influencers":[]},
            "unique_cnt": 0,
            "keep_days_mean": 0.0,
        }
    s, e = weekly_window(df)
    return aggregate_source(df, src, s, e)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="all", help="oy_kor, oy_global, amazon_us, qoo10_jp, daiso_kr, all")
    ap.add_argument("--data-dir", default="./data/daily")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    sources = SRC_ORDER if args.src == "all" else [args.src]
    out = {}
    for s in sources:
        if s not in SRC_META:
            continue
        print(f"[run] {s}")
        out[s] = run_for_source(s, data_dir)

    Path("weekly_summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("== weekly_summary.json saved ==")
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
