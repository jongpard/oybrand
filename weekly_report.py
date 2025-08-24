# report_agg.py
# -*- coding: utf-8 -*-
import os, re, glob, json
import pandas as pd
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

# ===== 1) 소스/키/표준 컬럼 =====
SOURCE_KEY_MAP = {
    "oy_kor": "goodsNo",
    "oy_global": "productId",
    "amazon_us": "asin",
    "qoo10_jp": "product_code",
    "daiso_kr": "pdNo",
}

VALID_SOURCES = set(SOURCE_KEY_MAP.keys())

def norm_source(s: str) -> str:
    s = (s or "").strip().lower()
    # 흔한 변형들 통일
    s = s.replace("oliveyoung_korea", "oy_kor").replace("oliveyoung_global", "oy_global")
    s = s.replace("oy_korea", "oy_kor").replace("amazon", "amazon_us")
    s = s.replace("qoo10", "qoo10_jp").replace("daiso", "daiso_kr")
    return s

def clean_key(x: str) -> str:
    x = str(x).strip()
    # 키에서 쓸데없는 문자 제거
    x = re.sub(r"[^A-Za-z0-9_\-]", "", x)
    return x.upper()  # 아마존 asin 대문자 통일

# ===== 2) 파일 로딩/표준화 =====
def load_daily_csvs(folder: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    folder 하위의 모든 CSV 중 날짜 컬럼이 [start_date, end_date]에 포함되는 데이터만 로드.
    CSV 컬럼 가정: date, source, rank, product, brand, url, price, currency, (각 소스별 키 컬럼 존재 혹은 url에서 추출)
    """
    paths = glob.glob(os.path.join(folder, "*.csv"))
    rows = []
    d0 = pd.to_datetime(start_date)
    d1 = pd.to_datetime(end_date)
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if "date" not in df.columns:
            continue
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= d0) & (df["date"] <= d1)].copy()
        if not len(df):
            continue

        # 소스 통일
        if "source" not in df.columns:
            continue
        df["source"] = df["source"].map(norm_source)

        # 표준 컬럼 보정
        for col in ["product", "brand", "url", "currency"]:
            if col not in df.columns:
                df[col] = None
        if "rank" in df.columns:
            df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
        else:
            continue

        # 키 보정: 우선 각 소스의 키 컬럼이 있으면 사용, 없으면 URL에서 추출
        for src, keycol in SOURCE_KEY_MAP.items():
            mask = df["source"].eq(src)
            if not mask.any():
                continue
            if keycol not in df.columns or df.loc[mask, keycol].isna().all():
                # URL에서 추출
                df.loc[mask, keycol] = df.loc[mask, "url"].map(lambda u: extract_key_from_url(src, u))
            df.loc[mask, "key"] = df.loc[mask, keycol].map(clean_key)

        rows.append(df)
    if not rows:
        return pd.DataFrame(columns=[
            "date","source","rank","product","brand","url","price","currency","key"
        ])
    out = pd.concat(rows, ignore_index=True)
    out = out[out["source"].isin(VALID_SOURCES)]
    out = out.dropna(subset=["rank","key"])
    return out

def extract_key_from_url(src: str, url: str) -> str:
    url = str(url or "")
    if src == "oy_kor":
        m = re.search(r"goodsNo=([0-9A-Za-z\-]+)", url)
        return m.group(1) if m else ""
    if src == "oy_global":
        m = re.search(r"productId=([0-9A-Za-z\-]+)", url)
        return m.group(1) if m else ""
    if src == "amazon_us":
        m = re.search(r"/([A-Z0-9]{10})(?:[/?]|$)", url.upper())
        return m.group(1) if m else ""
    if src == "qoo10_jp":
        m = re.search(r"product_code=([0-9A-Za-z\-]+)", url)
        return m.group(1) if m else ""
    if src == "daiso_kr":
        m = re.search(r"pdNo=([0-9A-Za-z\-]+)", url)
        return m.group(1) if m else ""
    return ""

# ===== 3) 주간/월간 윈도우 =====
def week_window(end_date_kst: datetime) -> tuple[str,str]:
    # KST 기준 월(0)~일(6) 주차. end_date가 일요일이면 그 주의 월~일 반환
    end_date_kst = end_date_kst.astimezone(KST)
    end_w = end_date_kst - timedelta(days=end_date_kst.weekday() - 6)  # 일요일로 보정
    start_w = end_w - timedelta(days=6)
    return start_w.strftime("%Y-%m-%d"), end_w.strftime("%Y-%m-%d")

def month_window(end_date_kst: datetime) -> tuple[str,str]:
    end_date_kst = end_date_kst.astimezone(KST)
    end = end_date_kst.replace(day=1) + timedelta(days=32)
    end = end.replace(day=1) - timedelta(days=1)  # 해당월 말일
    start = end.replace(day=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

# ===== 4) 집계 =====
def aggregate(df: pd.DataFrame, min_days:int=3, topn:int=100) -> dict:
    """
    반환: {source: {'top10': DataFrame, 'brand_share': DataFrame}}
    """
    result = {}
    if df.empty:
        for s in VALID_SOURCES:
            result[s] = {"top10": pd.DataFrame(), "brand_share": pd.DataFrame()}
        return result

    # 각 소스별 처리
    for src in sorted(df["source"].unique()):
        keycol = "key"
        d = df[df["source"].eq(src)].copy()
        if d.empty:
            result[src] = {"top10": pd.DataFrame(), "brand_share": pd.DataFrame()}
            continue

        # 상위 노출만 사용 (topn)
        d = d[pd.to_numeric(d["rank"], errors="coerce")<=topn].copy()

        # 최근 이름/URL 폴백 우선순위
        latest = (d.sort_values(["date"])
                    .groupby(keycol, as_index=False)
                    .agg({
                        "product": "last",
                        "brand": "last",
                        "url": "last",
                        "currency": "last"
                    }))
        # 제품명 폴백
        latest["product"] = latest["product"].fillna("")
        latest["brand"] = latest["brand"].fillna("")
        latest["product"] = latest.apply(
            lambda r: r["product"] if r["product"] else (r["brand"] if r["brand"] else r[keycol]),
            axis=1
        )

        # 주간 평균/등장일
        agg = (d.groupby(keycol, as_index=False)
                 .agg(mean_rank=("rank","mean"),
                      days=("rank","count"),
                      best=("rank","min")))

        # 최소 등장일 필터
        agg = agg[agg["days"]>=min_days].copy()

        # 조인(키 타입 통일)
        latest[keycol] = latest[keycol].map(clean_key)
        agg[keycol] = agg[keycol].map(clean_key)
        top = (agg.merge(latest, on=keycol, how="left")
                  .sort_values(["mean_rank","best"])
                  .head(10)
               )

        # 브랜드 점유율 (주간 노출수 기준, 동일키 중복일 모두 카운트)
        brand = d.copy()
        brand["brand"] = brand["brand"].fillna("기타")
        brand_cnt = (brand.groupby("brand", as_index=False)
                          .agg(count=("rank","count")))
        brand_cnt = brand_cnt.sort_values("count", ascending=False)

        result[src] = {"top10": top, "brand_share": brand_cnt}
    return result

# ===== 5) 슬랙용 포맷 =====
def arrow(delta:int) -> str:
    if delta > 0: return f"▼{abs(delta)}"
    if delta < 0: return f"▲{abs(delta)}"
    return "—"

def format_top10(df_top: pd.DataFrame, prev_top: pd.DataFrame|None=None) -> list[str]:
    if df_top is None or df_top.empty:
        return ["데이터 없음"]
    # 이전주 평균순위 대비 변화 계산(선택)
    prev_map = {}
    if isinstance(prev_top, pd.DataFrame) and not prev_top.empty:
        prev_map = dict(zip(prev_top["key"], prev_top["mean_rank"]))

    lines = []
    for i, r in enumerate(df_top.itertuples(), 1):
        prev_mean = prev_map.get(getattr(r,"key"), None)
        delta_text = ""
        if prev_mean is not None:
            diff = int(round(getattr(r,"mean_rank") - prev_mean))
            delta_text = f" {arrow(diff)}"

        nm = getattr(r,"product") or getattr(r,"key")
        url = getattr(r,"url") or ""
        name_txt = f"<{url}|{nm}>" if url else nm
        lines.append(f"{i}. {name_txt} (등장 {int(r.days)}일){delta_text}")
    return lines

def format_brand_share(df_brand: pd.DataFrame, topk:int=12) -> list[str]:
    if df_brand is None or df_brand.empty:
        return ["데이터 없음"]
    df = df_brand.head(topk)
    return [f"{r.brand} {int(r.count)}개" for r in df.itertuples()]

# ===== 6) 엔드포인트 =====
def build_summary(data_folder:str, mode:str="week", end_date:str|None=None,
                  min_days:int=3, topn:int=100, prev_folder:str|None=None) -> dict:
    """
    반환: {source: {'top10_lines': [...], 'brand_lines': [...], 'range': 'YYYY-MM-DD~YYYY-MM-DD'}}
    """
    if end_date:
        end_dt = datetime.fromisoformat(end_date).astimezone(KST)
    else:
        end_dt = datetime.now(tz=KST)

    if mode == "week":
        start, end = week_window(end_dt)
    elif mode == "month":
        start, end = month_window(end_dt)
    else:
        raise ValueError("mode must be 'week' or 'month'")

    df = load_daily_csvs(data_folder, start, end)
    agg_now = aggregate(df, min_days=min_days, topn=topn)

    # 이전 기간(증감용) 선택
    prev_map = {}
    if prev_folder:
        if mode == "week":
            prev_end = datetime.fromisoformat(end) - timedelta(days=7)
            pstart, pend = week_window(prev_end)
        else:
            prev_end = (datetime.fromisoformat(end).replace(day=1) - timedelta(days=1))
            pstart, pend = month_window(prev_end)
        df_prev = load_daily_csvs(prev_folder, pstart, pend)
        prev_map = aggregate(df_prev, min_days=min_days, topn=topn)

    out = {}
    for src in VALID_SOURCES:
        top = agg_now[src]["top10"]
        prev_top = prev_map.get(src, {}).get("top10") if prev_map else None
        top_lines = format_top10(top, prev_top)
        brand_lines = format_brand_share(agg_now[src]["brand_share"])
        out[src] = {"top10_lines": top_lines, "brand_lines": brand_lines, "range": f"{start}~{end}"}
    return out

# ===== 7) 로컬 테스트 =====
if __name__ == "__main__":
    # 예시
    folder = "./data/daily"          # 일간 CSV 위치
    prev_folder = "./data/daily"     # 동일 폴더에서 이전주/이전월 비교
    res = build_summary(folder, mode="week", prev_folder=prev_folder)
    print(json.dumps(res, ensure_ascii=False, indent=2))
