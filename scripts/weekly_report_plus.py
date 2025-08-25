#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
weekly_report_plus.py
- 소스별(oy_kor, oy_global, amazon_us, qoo10_jp, daiso_kr) 주간 집계 & Slack 메시지 생성
- 입력: --src {oy_kor|oy_global|amazon_us|qoo10_jp|daiso_kr|all} --data-dir ./data/daily
- 출력:
  - slack_{src}.txt                     (슬랙에 바로 전송 가능한 텍스트)
  - weekly_summary_{src}.json          (HTML 렌더/검수용 요약 데이터)
설계 요점
- 최근 주(월~일)와 전주(월~일)를 자동 산출
- TopN: 소스별 100/200
- Top10: (유지 일수 desc, 평균순위 asc) 정렬
- 브랜드 개수(일평균)
- 인앤아웃(교체): 일평균 IN 개수만 표기  ex) "일평균 31.0개"
- 히어로(>=3일 유지, 전주 미등장), 반짝(<=2일)
- 키워드: 마케팅/인플루언서/성분  → 가로 나열 ( · 구분)
  · 마케팅 키워드 완전 분리: 올영픽, PICK, 특가, 세트, 기획, 1+1, 증정, 한정, NEW
  · 인플루언서: oy_kor만 집계
- 히어로/반짝은 세로 표기, 각 품목에 URL 하이퍼링크가 붙도록 JSON에도 url 저장
"""

from __future__ import annotations
import argparse
import dataclasses
import json
import os
import re
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set

import pandas as pd
from dateutil.parser import parse as dtparse

# -----------------------------
# 소스 스펙/파일명 패턴/TopN
# -----------------------------

SRC_SPECS = {
    "oy_kor": {
        "title": "올리브영 국내 Top100",
        "topn": 100,
        "file_hint": ["올리브영_랭킹"],
        "key": "goodsNo",  # URL query key
    },
    "oy_global": {
        "title": "올리브영 글로벌 Top100",
        "topn": 100,
        "file_hint": ["올리브영글로벌_랭킹"],
        "key": "productId",
    },
    "amazon_us": {
        "title": "아마존 US Top100",
        "topn": 100,
        "file_hint": ["아마존US_뷰티_랭킹"],
        "key": "asin",
    },
    "qoo10_jp": {
        "title": "큐텐 재팬 뷰티 Top200",
        "topn": 200,
        "file_hint": ["큐텐재팬_뷰티_랭킹"],
        "key": "product_code",
    },
    "daiso_kr": {
        "title": "다이소몰 뷰티/위생 Top200",
        "topn": 200,
        "file_hint": ["다이소몰_뷰티위생_일간"],
        "key": "pdNo",
    },
}

# -----------------------------
# 마케팅/인플/성분 키워드 (정규식)
#  - 1+1, 증정 완전 분리
#  - PICK(콜라보) vs 올영픽 분리
# -----------------------------

def rx(p): return re.compile(p, re.I)

PAT_MARKETING = {
    "올영픽": rx(r"(?:^|\s)올영픽(?:\s|$)|올리브영\s*픽"),
    "PICK":   rx(r"\bPICK\b"),
    "특가":   rx(r"(특가|핫딜|딜|세일|할인)"),
    "세트":   rx(r"(세트|패키지|트리오|듀오|세트킷|키트|킷\b)"),
    "기획":   rx(r"(기획|기획전)"),
    "1+1":    rx(r"(?:^|\s)1\+1(?:\s|$)"),
    "증정":   rx(r"(증정|사은품)"),
    "한정":   rx(r"(한정|리미티드)"),
    "NEW":    rx(r"\bNEW\b|(?<!리)뉴\b"),
}

# 인플루언서: oy_kor만
INFLUENCERS = [
    # 예시(지속 확장 가능, 'Pick'과는 무관하게 이름만 잡아도 집계)
    "유인", "어프어프", "Olad", "올라드", "박보영", "윈터", "하츠투하츠", "하루",
    "허윤진", "이유정", "문가영", "카리나", "장원영", "수지", "아이브", "뉴진스",
]

# 성분(기본 리스트 + 대문자 약어류)
INGREDIENTS = [
    "레티놀", "비타민C", "콜라겐", "히알루론산", "세라마이드", "판테놀",
    "마데카소사이드", "병풀", "티트리", "녹차", "쑥", "연어", "펩타이드",
    "PDRN", "AHA", "BHA", "PHA", "니아신아마이드", "아젤라익", "징크", "유산균",
]


# -----------------------------
# 데이터 모델
# -----------------------------

@dataclass
class ItemStat:
    sku: str
    raw_name: str
    url: str
    days: int
    ranks: List[int]

    @property
    def avg_rank(self) -> float:
        return float(sum(self.ranks)) / max(1, len(self.ranks))

    @property
    def min_rank(self) -> int:
        return min(self.ranks) if self.ranks else 9999


# -----------------------------
# 유틸
# -----------------------------

def to_date(s: str) -> datetime:
    try:
        return dtparse(s).date()
    except Exception:
        return datetime.strptime(s, "%Y-%m-%d").date()

def monday_of(date_: datetime) -> datetime:
    return date_ - timedelta(days=date_.weekday())  # Monday=0

def daterange(start: datetime, end: datetime) -> List[datetime]:
    d = start
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out

def get_week_range_for_latest(dates: List[datetime]) -> Tuple[List[datetime], List[datetime]]:
    """파일들 안의 가장 최신 날짜 기준으로 '그 주 월~일', '그 전주 월~일' 범위 반환."""
    if not dates:
        return [], []
    last = max(dates)
    cur_mon = monday_of(last)
    cur_sun = cur_mon + timedelta(days=6)
    prev_mon = cur_mon - timedelta(days=7)
    prev_sun = prev_mon + timedelta(days=6)
    return daterange(cur_mon, cur_sun), daterange(prev_mon, prev_sun)

def extract_id_from_url(url: str, key: str) -> str:
    if not isinstance(url, str):
        return ""
    # 단순 query search
    m = re.search(r"[?&]{}=([^&#]+)".format(re.escape(key)), url)
    if m:
        return m.group(1)
    # 아마존 asin이 path에 있을 수 있음
    if key == "asin":
        m2 = re.search(r"/([A-Z0-9]{10})(?:[/?#]|$)", url)
        if m2:
            return m2.group(1)
    return ""

def pick_first(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


# -----------------------------
# 로딩/스캔
# -----------------------------

def discover_files(data_dir: str, src: str) -> Dict[datetime, str]:
    """data_dir에서 src에 해당하는 파일을 날짜별로 매핑."""
    hints = SRC_SPECS[src]["file_hint"]
    files = {}
    for fn in os.listdir(data_dir):
        if not fn.endswith(".csv"):
            continue
        if not any(h in fn for h in hints):
            continue
        # 파일명에서 날짜 추출
        m = re.search(r"(\d{4}-\d{2}-\d{2})", fn)
        if not m:
            continue
        d = to_date(m.group(1))
        files[d] = os.path.join(data_dir, fn)
    return dict(sorted(files.items()))

def load_day_df(path: str, src: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # 표준화 컬럼 추론
    url_col = None
    for c in ["url", "URL", "link", "상품URL", "상품링크"]:
        if c in df.columns:
            url_col = c; break
    name_col = None
    for c in ["raw_name", "name", "상품명", "title", "Title"]:
        if c in df.columns:
            name_col = c; break
    brand_col = None
    for c in ["brand", "브랜드", "Brand"]:
        if c in df.columns:
            brand_col = c; break
    rank_col = None
    for c in ["rank", "순위", "랭킹"]:
        if c in df.columns:
            rank_col = c; break

    if rank_col is None:
        raise KeyError("rank/순위 컬럼이 필요합니다: " + path)

    df["_rank"] = df[rank_col].astype(int)

    df["_url"] = df[url_col] if url_col else ""
    df["_raw_name"] = df[name_col] if name_col else ""
    df["_brand"] = df[brand_col] if brand_col else ""

    # SKU 키 결정
    key = SRC_SPECS[src]["key"]
    def _id(row):
        return extract_id_from_url(str(row["_url"]), key) or str(row.get(key, "") or "")
    df["_sku"] = df.apply(_id, axis=1)

    # TopN 제한
    topn = SRC_SPECS[src]["topn"]
    df = df.sort_values("_rank").head(topn).reset_index(drop=True)
    return df[["_sku", "_raw_name", "_url", "_brand", "_rank"]]


# -----------------------------
# 주간 집계
# -----------------------------

def weekly_stats(day_dfs: Dict[datetime, pd.DataFrame]) -> Dict[str, ItemStat]:
    """일자별 DF -> SKU별 주간 스탯"""
    by_sku: Dict[str, ItemStat] = {}
    for d, df in day_dfs.items():
        for row in df.itertuples(index=False):
            sku = row._sku
            if not sku:
                # URL 키가 없으면 raw_name 기준 fallback(희귀 케이스)
                sku = f"RAW::{row._raw_name}"
            it = by_sku.get(sku)
            if not it:
                it = ItemStat(sku=sku, raw_name=row._raw_name, url=row._url, days=0, ranks=[])
                by_sku[sku] = it
            it.days += 1
            it.ranks.append(int(row._rank))
            # URL/이름 갱신(가장 최근 것 우선)
            if row._url:
                it.url = row._url
            if row._raw_name:
                it.raw_name = row._raw_name
    return by_sku

def daily_sets(day_dfs: Dict[datetime, pd.DataFrame]) -> List[Tuple[datetime, Set[str]]]:
    out = []
    for d, df in sorted(day_dfs.items()):
        out.append((d, set(df["_sku"].tolist())))
    return out

def average_inout(day_sets: List[Tuple[datetime, Set[str]]]) -> float:
    if len(day_sets) < 2:
        return 0.0
    changes = []
    for (d1, s1), (d2, s2) in zip(day_sets, day_sets[1:]):
        ins = len(s2 - s1)
        changes.append(ins)
    if not changes:
        return 0.0
    return round(sum(changes) / len(changes), 1)

def brand_daily_average(day_dfs: Dict[datetime, pd.DataFrame]) -> List[str]:
    # 일자별 브랜드 카운트 → 주간 평균
    per_day = []
    for d, df in day_dfs.items():
        c = Counter([str(x).strip() for x in df["_brand"].fillna("").tolist() if str(x).strip()])
        per_day.append(c)
    if not per_day:
        return ["데이터 없음"]

    brands = set()
    for c in per_day:
        brands.update(c.keys())
    avg = []
    for b in brands:
        avg_cnt = sum(c.get(b, 0) for c in per_day) / len(per_day)
        avg.append((b, avg_cnt))
    avg.sort(key=lambda x: (-x[1], x[0]))
    lines = [f"{b} {round(v,1)}개/일" for b, v in avg[:20]]  # 상위 20개만
    return lines or ["데이터 없음"]

def top10_lines_from_stats(stats: Dict[str, ItemStat]) -> List[str]:
    # (유지일수 desc, 평균순위 asc) 정렬
    arr = sorted(stats.values(), key=lambda s: (-s.days, s.avg_rank, s.min_rank))
    out = []
    for i, it in enumerate(arr[:10], start=1):
        tail = f"(유지 {it.days}일 · 평균 {it.avg_rank:.1f}위)"
        if it.url:
            nm = f"<{it.url}|{it.raw_name}>"
        else:
            nm = it.raw_name
        out.append(f"{i}. {nm} {tail}")
    return out or ["데이터 없음"]

def hero_and_flash(stats: Dict[str, ItemStat], prev_stats: Dict[str, ItemStat]) -> Tuple[List[ItemStat], List[ItemStat]]:
    # 히어로: 이번주 3일 이상 & 전주에 없던 SKU
    heroes = [s for sku, s in stats.items() if s.days >= 3 and sku not in prev_stats]
    flashes = [s for sku, s in stats.items() if s.days <= 2]
    heroes.sort(key=lambda s: (-s.days, s.avg_rank, s.min_rank))
    flashes.sort(key=lambda s: (s.days, s.avg_rank, s.min_rank))
    return heroes[:10], flashes[:10]


# -----------------------------
# 키워드 집계
# -----------------------------

def keyword_stats(stats: Dict[str, ItemStat], src: str) -> Dict[str, Dict[str, int]]:
    """
    unique: 주간 유니크 SKU 수
    marketing: {키워드: 카운트} (SKU 기준)
    influencers: {이름: 카운트}  (oy_kor만)
    ingredients: {성분: 카운트}
    """
    unique = len(stats)
    mk = Counter()
    infl = Counter()
    ing = Counter()

    for s in stats.values():
        name = s.raw_name or ""
        # 마케팅
        for k, r in PAT_MARKETING.items():
            if r.search(name):
                mk[k] += 1
        # 인플 (oy_kor만)
        if src == "oy_kor":
            for p in INFLUENCERS:
                if re.search(re.escape(p), name, re.I):
                    infl[p] += 1
        # 성분
        for p in INGREDIENTS:
            if re.search(r"\b" + re.escape(p) + r"\b", name, re.I):
                ing[p] += 1

    # 정렬
    mk = Counter(dict(sorted(mk.items(), key=lambda x: (-x[1], x[0]))))
    infl = Counter(dict(sorted(infl.items(), key=lambda x: (-x[1], x[0]))))
    ing = Counter(dict(sorted(ing.items(), key=lambda x: (-x[1], x[0]))))

    return {
        "unique": unique,
        "marketing": dict(mk),
        "influencers": dict(infl),
        "ingredients": dict(ing),
    }

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
        lines.append("• *마케팅 키워드* " + " · ".join(mk_parts))

    if kw["influencers"]:
        infl_parts = [f"{k} {v}개" for k, v in kw["influencers"].items()]
        lines.append("• *인플루언서* " + " · ".join(infl_parts))

    if kw["ingredients"]:
        ing_parts = [f"{k} {v}개" for k, v in kw["ingredients"].items()]
        lines.append("• *성분 키워드* " + " · ".join(ing_parts))

    return "\n".join(lines)


# -----------------------------
# Slack/JSON 빌더
# -----------------------------

def build_slack(
    src: str,
    range_str: str,
    top10_lines: List[str],
    brand_lines: List[str],
    inout_avg: float,
    heroes: List[ItemStat],
    flashes: List[ItemStat],
    kw_text: str,
    unique_cnt: int,
    keep_days_mean: float,
) -> str:
    title = SRC_SPECS[src]["title"]
    lines: List[str] = []
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
    # 히어로/반짝: 세로 + 링크
    lines.append("🆕 *신규 히어로(≥3일 유지)*")
    if not heroes:
        lines.append("없음")
    else:
        for st in heroes:
            nm = f"<{st.url}|{st.raw_name}>" if st.url else st.raw_name
            lines.append(f"- {nm} (유지 {st.days}일 · 평균 {st.avg_rank:.1f}위)")
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
    if kw_text:
        lines.append(kw_text)
    return "\n".join(lines)

def item_to_dict(st: ItemStat) -> Dict[str, any]:
    return {
        "name": st.raw_name,
        "url": st.url,
        "days": st.days,
        "avg": round(st.avg_rank, 1),
    }


# -----------------------------
# 파이프라인
# -----------------------------

def run_for_source(src: str, args) -> Tuple[str, str]:
    """
    반환: (슬랙 텍스트, 요약 json 문자열)
    """
    files = discover_files(args.data_dir, src)
    if not files:
        # 빈 파일 생성
        empty = {
            "range": "데이터 없음",
            "title": SRC_SPECS[src]["title"],
            "topn": SRC_SPECS[src]["topn"],
            "top10_items": [],
            "brand_lines": ["데이터 없음"],
            "inout_avg": 0.0,
            "heroes": [],
            "flashes": [],
            "kw": {"unique": 0, "marketing": {}, "influencers": {}, "ingredients": {}},
            "unique_cnt": 0,
            "keep_days_mean": 0.0,
        }
        slack = f"📈 *주간 리포트 · {SRC_SPECS[src]['title']}*  \n데이터 없음"
        return slack, json.dumps(empty, ensure_ascii=False, indent=2)

    # 날짜 범위 산출 (최근 주 & 전주)
    all_dates = list(files.keys())
    cur_week, prev_week = get_week_range_for_latest(all_dates)

    def pick_week(day_list: List[datetime]) -> Dict[datetime, pd.DataFrame]:
        m = {}
        for d in day_list:
            if d in files:
                m[d] = load_day_df(files[d], src)
        return m

    cur_daydfs = pick_week(cur_week)
    prev_daydfs = pick_week(prev_week)

    range_str = f"{cur_week[0]}~{cur_week[-1]}" if cur_week else "데이터 없음"
    if not cur_daydfs:
        # 데이터 없음 처리
        empty = {
            "range": range_str,
            "title": SRC_SPECS[src]["title"],
            "topn": SRC_SPECS[src]["topn"],
            "top10_items": [],
            "brand_lines": ["데이터 없음"],
            "inout_avg": 0.0,
            "heroes": [],
            "flashes": [],
            "kw": {"unique": 0, "marketing": {}, "influencers": {}, "ingredients": {}},
            "unique_cnt": 0,
            "keep_days_mean": 0.0,
        }
        slack = f"📈 *주간 리포트 · {SRC_SPECS[src]['title']} ({range_str})*\n데이터 없음"
        return slack, json.dumps(empty, ensure_ascii=False, indent=2)

    # 주간 스탯
    cur_stats = weekly_stats(cur_daydfs)
    prev_stats = weekly_stats(prev_daydfs) if prev_daydfs else {}

    top10_lines = top10_lines_from_stats(cur_stats)
    brand_lines = brand_daily_average(cur_daydfs)
    inout_avg = average_inout(daily_sets(cur_daydfs))
    unique_cnt = len(cur_stats)
    keep_days_mean = round(sum(s.days for s in cur_stats.values()) / max(1, unique_cnt), 1)

    heroes, flashes = hero_and_flash(cur_stats, prev_stats)
    kw = keyword_stats(cur_stats, src)
    kw_text = format_kw_for_slack(kw)

    slack_text = build_slack(
        src=src,
        range_str=range_str,
        top10_lines=top10_lines,
        brand_lines=brand_lines,
        inout_avg=inout_avg,
        heroes=heroes,
        flashes=flashes,
        kw_text=kw_text,
        unique_cnt=unique_cnt,
        keep_days_mean=keep_days_mean,
    )

    summary = {
        "range": range_str,
        "title": SRC_SPECS[src]["title"],
        "topn": SRC_SPECS[src]["topn"],
        "top10_items": top10_lines,
        "brand_lines": brand_lines,
        "inout_avg": inout_avg,
        "heroes": [item_to_dict(x) for x in heroes],
        "flashes": [item_to_dict(x) for x in flashes],
        "kw": kw,
        "unique_cnt": unique_cnt,
        "keep_days_mean": keep_days_mean,
    }

    return slack_text, json.dumps(summary, ensure_ascii=False, indent=2)


# -----------------------------
# main/CLI
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="all",
                        choices=["oy_kor", "oy_global", "amazon_us", "qoo10_jp", "daiso_kr", "all"])
    parser.add_argument("--data-dir", default="./data/daily")
    args = parser.parse_args()

    targets = [args.src] if args.src != "all" else list(SRC_SPECS.keys())

    os.makedirs(".", exist_ok=True)
    for src in targets:
        slack_text, summary_json = run_for_source(src, args)

        # 파일 출력
        slack_fn = f"slack_{src}.txt"
        json_fn = f"weekly_summary_{src}.json"
        with open(slack_fn, "w", encoding="utf-8") as f:
            f.write(slack_text)
        with open(json_fn, "w", encoding="utf-8") as f:
            f.write(summary_json)

        print(f"[OK] {src}: {slack_fn}, {json_fn} 생성")


if __name__ == "__main__":
    main()
