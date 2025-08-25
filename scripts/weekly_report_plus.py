# scripts/weekly_report_plus.py
# -*- coding: utf-8 -*-

"""
주간 리포트 생성 (SKU 표준화 포함)
- 사용 예: python scripts/weekly_report_plus.py --src all --data-dir ./data/daily
- 출력:
  - slack_oy_kor.txt, slack_oy_global.txt, slack_amazon_us.txt, slack_qoo10_jp.txt, slack_daiso_kr.txt
  - weekly_summary_oy_kor.json, ... (소스별)
"""

from __future__ import annotations
import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from urllib.parse import urlparse, parse_qs


# ──────────────────────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────────────────────

SOURCES = ["oy_kor", "oy_global", "amazon_us", "qoo10_jp", "daiso_kr"]

# 파일명 패턴 (일자 YYYY-MM-DD 파싱용)
FILENAME_PATTERNS = {
    "oy_kor":       r"^올리브영_랭킹_(\d{4}-\d{2}-\d{2})\.csv$",
    "oy_global":    r"^올리브영글로벌_랭킹_(\d{4}-\d{2}-\d{2})\.csv$",
    "amazon_us":    r"^아마존US_뷰티_랭킹_(\d{4}-\d{2}-\d{2})\.csv$",
    "qoo10_jp":     r"^큐텐재팬_뷰티_랭킹_(\d{4}-\d{2}-\d{2})\.csv$",
    "daiso_kr":     r"^다이소몰_뷰티위생_일간_(\d{4}-\d{2}-\d{2})\.csv$",
}

# 소스별 Slack/JSON 제목
TITLES = {
    "oy_kor":    "올리브영 국내 Top100",
    "oy_global": "올리브영 글로벌 Top100",
    "amazon_us": "아마존 US Top100",
    "qoo10_jp":  "큐텐 재팬 뷰티 Top200",
    "daiso_kr":  "다이소몰 뷰티/위생 Top200",
}

TOPN = {
    "oy_kor": 100,
    "oy_global": 100,
    "amazon_us": 100,
    "qoo10_jp": 200,
    "daiso_kr": 200,
}


# ──────────────────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────────────────

def this_week_range(today: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """
    이번 주 월요일~일요일 범위(로컬 날짜 기준).
    (GitHub Actions에서는 UTC, 한국기준이면 KST 보정이 필요하지만 여기서는 단순 계산)
    """
    if today is None:
        today = datetime.utcnow()
    # 월요일=0
    monday = today - timedelta(days=today.weekday())
    monday = datetime(monday.year, monday.month, monday.day)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def parse_date_from_filename(name: str, src: str) -> Optional[datetime]:
    pat = FILENAME_PATTERNS.get(src)
    if not pat:
        return None
    m = re.match(pat, name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except Exception:
        return None


def read_csv_safe(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    except Exception:
        try:
            return pd.read_csv(path, encoding="utf-8", low_memory=False)
        except Exception:
            return None


def rename_common_columns(df: pd.DataFrame, src: str) -> pd.DataFrame:
    """
    소스별로 들쭉날쭉한 컬럼명을 공통 키로 맞춘다.
    최소한 아래 키만 맞춘다:
    - date
    - rank
    - product_name
    - brand
    - url
    - asin / product_code (그대로 유지)
    """
    df = df.copy()

    # name -> product_name
    if "product_name" not in df.columns and "name" in df.columns:
        df = df.rename(columns={"name": "product_name"})

    # rank 형식 보정
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    # date 형식 보정
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # brand 없을 수도 있다 (그냥 둠)
    # url 없는 경우는 거의 없음 (없으면 빈 문자열)
    if "url" not in df.columns:
        df["url"] = None

    return df


def extract_query_param(url: str, key: str) -> Optional[str]:
    if not isinstance(url, str) or not url:
        return None
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get(key)
        return v[0] if v else None
    except Exception:
        return None


def ensure_sku_column(df: pd.DataFrame, src: str) -> pd.DataFrame:
    """
    소스별로 공통 sku 컬럼 생성:
      oy_kor    -> url?goodsNo
      oy_global -> url?productId
      amazon_us -> asin
      qoo10_jp  -> product_code
      daiso_kr  -> url?pdNo
    """
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    if src == "oy_kor":
        if "sku" not in df.columns:
            df["sku"] = df["url"].apply(lambda u: extract_query_param(u, "goodsNo"))
    elif src == "oy_global":
        if "sku" not in df.columns:
            df["sku"] = df["url"].apply(lambda u: extract_query_param(u, "productId"))
    elif src == "amazon_us":
        if "sku" not in df.columns:
            df["sku"] = df.get("asin")
    elif src == "qoo10_jp":
        if "sku" not in df.columns:
            df["sku"] = df.get("product_code")
    elif src == "daiso_kr":
        if "sku" not in df.columns:
            df["sku"] = df["url"].apply(lambda u: extract_query_param(u, "pdNo"))

    # 문자열 정리
    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip()
        df["sku"] = df["sku"].replace({"": None, "None": None})
    else:
        # 그래도 없으면 url을 fallback (중복 가능성 큼)
        df["sku"] = df.get("url")

    return df


# ──────────────────────────────────────────────────────────────────────────────
# 로딩/필터링
# ──────────────────────────────────────────────────────────────────────────────

def list_source_files(data_dir: Path, src: str, start: datetime, end: datetime) -> List[Path]:
    files: List[Path] = []
    for p in sorted(data_dir.glob("*.csv")):
        d = parse_date_from_filename(p.name, src)
        if d is None:
            continue
        if start.date() <= d.date() <= end.date():
            files.append(p)
    return files


def load_week_df(src: str, data_dir: Path, start: datetime, end: datetime) -> pd.DataFrame:
    files = list_source_files(data_dir, src, start, end)
    frames: List[pd.DataFrame] = []
    for f in files:
        df = read_csv_safe(f)
        if df is None or len(df) == 0:
            continue
        df = rename_common_columns(df, src)

        # 파일명 일자를 date 결측에 채워넣기 (없으면)
        if "date" in df.columns:
            # 일부 파일에 date가 전부 NaT인 경우 보완
            if df["date"].isna().all():
                d = parse_date_from_filename(f.name, src)
                if d:
                    df["date"] = d.date()
        else:
            d = parse_date_from_filename(f.name, src)
            df["date"] = d.date() if d else None

        df = ensure_sku_column(df, src)

        # 기본 정렬/필터
        if "rank" in df.columns:
            df = df[df["rank"].notna()]
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    # 범위로 한 번 더 필터링
    out = out[(pd.to_datetime(out["date"], errors="coerce") >= start.date()) &
              (pd.to_datetime(out["date"], errors="coerce") <= end.date())]
    return out


# ──────────────────────────────────────────────────────────────────────────────
# 통계/요약
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class WeeklyStats:
    unique_cnt: int
    keep_days_mean: float
    topn_items: List[Dict]  # [{sku, name, brand, avg_rank, latest_rank, days}, ...]


def calc_weekly_stats(df: pd.DataFrame, src: str) -> WeeklyStats:
    if df is None or len(df) == 0:
        return WeeklyStats(unique_cnt=0, keep_days_mean=0.0, topn_items=[])

    # product_name 보정
    if "product_name" not in df.columns:
        df["product_name"] = None
    if "brand" not in df.columns:
        df["brand"] = None

    # sku 기준으로 일수/평균순위 계산
    g = df.groupby("sku", dropna=True)
    days = g["date"].nunique().rename("days")
    avg_rank = g["rank"].mean().rename("avg_rank")

    # 최신(최신일자) product_name/brand/rank
    df["dt"] = pd.to_datetime(df["date"], errors="coerce")
    latest_idx = df.sort_values(["sku", "dt"], ascending=[True, False]).groupby("sku").head(1).set_index("sku")
    latest_name = latest_idx["product_name"].rename("latest_name")
    latest_brand = latest_idx["brand"].rename("latest_brand")
    latest_rank = latest_idx["rank"].rename("latest_rank")

    stat_df = pd.concat([days, avg_rank, latest_name, latest_brand, latest_rank], axis=1).reset_index()
    stat_df = stat_df[stat_df["sku"].notna()]

    # 상위 TOPN 선정 (avg_rank 오름차순)
    n = TOPN.get(src, 100)
    stat_df = stat_df.sort_values(["avg_rank", "latest_rank"], ascending=[True, True]).head(10)

    items = []
    for _, row in stat_df.iterrows():
        items.append({
            "sku": row.get("sku"),
            "name": row.get("latest_name"),
            "brand": row.get("latest_brand"),
            "avg_rank": float(row.get("avg_rank")) if pd.notna(row.get("avg_rank")) else None,
            "latest_rank": int(row.get("latest_rank")) if pd.notna(row.get("latest_rank")) else None,
            "days": int(row.get("days")) if pd.notna(row.get("days")) else 0,
        })

    # 전체 유니크 / 평균 유지일
    unique_cnt = df["sku"].nunique(dropna=True)
    keep_days_mean = float(days.mean()) if not days.empty else 0.0

    return WeeklyStats(unique_cnt=unique_cnt, keep_days_mean=keep_days_mean, topn_items=items)


# ──────────────────────────────────────────────────────────────────────────────
# 출력 (Slack 텍스트 / JSON)
# ──────────────────────────────────────────────────────────────────────────────

def format_top10_lines(stats: WeeklyStats) -> List[str]:
    lines: List[str] = []
    if not stats.topn_items:
        lines.append("데이터 없음")
        return lines

    for i, it in enumerate(stats.topn_items, start=1):
        name = it.get("name") or "-"
        brand = it.get("brand") or "-"
        avg_rank = it.get("avg_rank")
        latest_rank = it.get("latest_rank")
        days = it.get("days", 0)
        # 예: "1. 브랜드 제품명 (유지 7일 · 평균 3.2위)"
        s = f"{i}. {name} (유지 {days}일 · 평균 {avg_rank:.1f}위)" if avg_rank else f"{i}. {name}"
        lines.append(s)
    return lines


def build_slack_text(src: str, start: datetime, end: datetime, stats: WeeklyStats) -> str:
    title = TITLES.get(src, src)
    period = f"{start.date()}~{end.date()}"
    top10_lines = format_top10_lines(stats)

    body = []
    body.append(f"📈 주간 리포트 · {title} ({period})")
    body.append("🏆 Top10")
    for ln in top10_lines:
        body.append(ln)
    body.append("")
    body.append("📦 통계")
    body.append(f"- Top{TOPN.get(src,100)} 등극 SKU : {stats.unique_cnt}개")
    body.append(f"- Top {TOPN.get(src,100)} 유지 평균 : {stats.keep_days_mean:.1f}일")
    return "\n".join(body)


def save_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")


def save_json(path: Path, obj: dict):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# 실행 본체
# ──────────────────────────────────────────────────────────────────────────────

def run_for_source(src: str, data_dir: Path, start: datetime, end: datetime):
    print(f"[run] {src}")
    cur_df = load_week_df(src, data_dir, start, end)
    stats = calc_weekly_stats(cur_df, src)

    # Slack 텍스트
    slack_text = build_slack_text(src, start, end, stats)
    save_text(Path(f"slack_{src}.txt"), slack_text)

    # 요약 JSON
    summary = {
        "title": TITLES.get(src, src),
        "range": f"{start.date()}~{end.date()}",
        "topn": TOPN.get(src, 100),
        "top10_items": stats.topn_items,
        "unique_cnt": stats.unique_cnt,
        "keep_days_mean": round(stats.keep_days_mean, 2),
    }
    save_json(Path(f"weekly_summary_{src}.json"), summary)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="all", help="oy_kor,oy_global,amazon_us,qoo10_jp,daiso_kr,all")
    ap.add_argument("--data-dir", default="./data/daily")
    args = ap.parse_args()

    if args.src == "all":
        targets = SOURCES
    else:
        targets = [s.strip() for s in args.src.split(",") if s.strip() in SOURCES]
        if not targets:
            print("유효한 --src 가 아닙니다. 가능: ", SOURCES, file=sys.stderr)
            sys.exit(1)

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"[ERR] DATA_DIR 없음: {data_dir}", file=sys.stderr)
        sys.exit(1)

    start, end = this_week_range()
    print(f"[scan] 기간 {start.date()} ~ {end.date()} | dir={data_dir}")

    for s in targets:
        run_for_source(s, data_dir, start, end)


if __name__ == "__main__":
    main()
