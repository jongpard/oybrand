# agg_plus.py
# -*- coding: utf-8 -*-
import os, re, glob, json, math
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict

KST = timezone(timedelta(hours=9))

SOURCE_KEY_MAP = {
    "oy_kor": "goodsNo",
    "oy_global": "productId",
    "amazon_us": "asin",
    "qoo10_jp": "product_code",
    "daiso_kr": "pdNo",
}
VALID_SOURCES = set(SOURCE_KEY_MAP.keys())

PROMO_PREFIX_PAT = re.compile(r"^(올영픽|올영 픽|[가-힣A-Za-z0-9]+특가|[A-Za-z]+ ?Pick)\s*[-\s·]*", re.IGNORECASE)

# 제품군 간단 룰(제품명 기준, 다중매칭되면 첫 일치 사용)
CATEGORY_RULES = [
    ("마스크팩", r"(마스크팩|팩|sheet mask|mask pack)"),
    ("선케어", r"(선크림|자외선|sun ?cream|sunscreen|uv)"),
    ("클렌저", r"(클렌징|클렌저|foam|cleanser|wash)"),
    ("토너/스킨", r"(토너|스킨|toner)"),
    ("에센스/세럼", r"(에센스|세럼|앰플|serum|essence|ampoule)"),
    ("로션/에멀전", r"(로션|에멀전|lotion|emulsion)"),
    ("크림", r"(크림|cream|moisturizer)"),
    ("립", r"(립|틴트|립밤|lip|tint|balm)"),
    ("아이", r"(아이크림|eye\s*cream|아이|아이패치)"),
    ("헤어", r"(샴푸|트리트먼트|헤어|hair)"),
    ("바디", r"(바디|body|바스)"),
    ("향수", r"(향수|퍼퓸|eau|parfum|perfume)"),
    ("도구/기기", r"(기기|디바이스|롤러|device|tool)"),
]

STOPWORDS = set("""
의 가 이 은 는 을 를 에 에서 으로 도 과 와 및 ( ) , . : · - & x X + the and or for of with
세트 1+1 2+1 10개입 20매 30g 50ml 100ml 200ml
""".split())

def norm_source(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("oliveyoung_korea","oy_kor").replace("oliveyoung_global","oy_global")
    s = s.replace("oy_korea","oy_kor")
    s = s.replace("amazon","amazon_us")
    s = s.replace("qoo10","qoo10_jp")
    s = s.replace("daiso","daiso_kr")
    return s

def clean_key(x: str) -> str:
    x = str(x).strip()
    x = re.sub(r"[^A-Za-z0-9_\-]", "", x)
    return x.upper()

def strip_promo_prefix(name: str) -> str:
    if not name: return name
    return PROMO_PREFIX_PAT.sub("", str(name)).strip()

def extract_key_from_url(src: str, url: str) -> str:
    url = str(url or "")
    if src == "oy_kor":
        m = re.search(r"goodsNo=([0-9A-Za-z\-]+)", url);  return m.group(1) if m else ""
    if src == "oy_global":
        m = re.search(r"productId=([0-9A-Za-z\-]+)", url);  return m.group(1) if m else ""
    if src == "amazon_us":
        m = re.search(r"/([A-Z0-9]{10})(?:[/?]|$)", url.upper());  return m.group(1) if m else ""
    if src == "qoo10_jp":
        m = re.search(r"product_code=([0-9A-Za-z\-]+)", url);  return m.group(1) if m else ""
    if src == "daiso_kr":
        m = re.search(r"pdNo=([0-9A-Za-z\-]+)", url);  return m.group(1) if m else ""
    return ""

def week_window_by_data(folder:str, src:str) -> tuple[str,str]|None:
    """해당 소스의 최신 날짜를 기준으로 월~일 주간 범위를 반환"""
    paths = glob.glob(os.path.join(folder, "*.csv"))
    dates = []
    for p in paths:
        try:
            df = pd.read_csv(p, usecols=["date","source"])
        except Exception:
            continue
        if df.empty: continue
        if "date" not in df.columns or "source" not in df.columns: continue
        df["source"] = df["source"].map(norm_source)
        df = df[df["source"].eq(src)]
        if df.empty: continue
        dates.append(pd.to_datetime(df["date"]))
    if not dates:
        return None
    maxd = pd.concat(dates).max()
    if pd.isna(maxd): return None
    maxd = pd.Timestamp(maxd)
    # KST 처리
    if maxd.tzinfo is None: maxd = maxd.tz_localize(KST)
    else: maxd = maxd.astimezone(KST)
    end_sun = maxd + timedelta(days=(6 - maxd.weekday()))
    start_mon = end_sun - timedelta(days=6)
    return start_mon.strftime("%Y-%m-%d"), end_sun.strftime("%Y-%m-%d")

def load_daily_csvs(folder: str, start_date: str, end_date: str, only_source: str|None=None) -> pd.DataFrame:
    paths = glob.glob(os.path.join(folder, "*.csv"))
    cols_std = ["date","source","rank","product","brand","url","price","currency","orig_price","discount_rate","key"]
    rows = []
    d0 = pd.to_datetime(start_date)
    d1 = pd.to_datetime(end_date)
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if "date" not in df.columns or "source" not in df.columns: 
            continue
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"]>=d0) & (df["date"]<=d1)].copy()
        if df.empty: 
            continue

        df["source"] = df["source"].map(norm_source)
        if only_source:
            df = df[df["source"].eq(only_source)]
            if df.empty: 
                continue

        for col in ["product","brand","url","currency","price","orig_price","discount_rate"]:
            if col not in df.columns: df[col] = None
        if "rank" not in df.columns:
            continue
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

        # 제품명 정리(프로모션 접두어 제거)
        df["product"] = df["product"].map(strip_promo_prefix)

        # 키 생성/정규화
        for src, keycol in SOURCE_KEY_MAP.items():
            mask = df["source"].eq(src)
            if not mask.any(): 
                continue
            if keycol not in df.columns or df.loc[mask, keycol].isna().all():
                df.loc[mask, keycol] = df.loc[mask,"url"].map(lambda u: extract_key_from_url(src,u))
            df.loc[mask,"key"] = df.loc[mask, keycol].map(clean_key)

        rows.append(df)

    if not rows:
        return pd.DataFrame(columns=cols_std)

    out = pd.concat(rows, ignore_index=True)
    out = out[out["source"].isin(VALID_SOURCES)]
    out = out.dropna(subset=["rank","key"])
    return out

def select_topn_per_source(src: str) -> int:
    if src in ("qoo10_jp","daiso_kr"): return 200
    return 100

def aggregate_week(df_cur: pd.DataFrame, df_prev: pd.DataFrame|None, src:str, min_days:int=3) -> dict:
    """주간 집계 + 비교/트렌드"""
    res = {
        "top10": pd.DataFrame(),
        "brand_share": pd.DataFrame(),
        "brand_share_delta": pd.DataFrame(),
        "in_count": 0, "out_count": 0,
        "in_avg_per_day": 0.0, "out_avg_per_day": 0.0,
        "new_heroes": pd.DataFrame(),
        "flash_items": pd.DataFrame(),
        "avg_discount": None,
        "avg_price": None,
        "cat_share": pd.DataFrame(),
        "keyword_share": pd.DataFrame(),
        "prev_top": pd.DataFrame(),
    }
    if df_cur.empty:
        return res

    topn = select_topn_per_source(src)
    d = df_cur[pd.to_numeric(df_cur["rank"], errors="coerce")<=topn].copy()

    # 최신 속성(이름/브랜드/URL/통화) 폴백
    latest = (d.sort_values("date")
                .groupby("key", as_index=False)
                .agg(product=("product","last"),
                     brand=("brand","last"),
                     url=("url","last"),
                     currency=("currency","last")))

    latest["product"] = latest["product"].fillna("")
    latest["brand"] = latest["brand"].fillna("")
    latest["product"] = latest.apply(
        lambda r: r["product"] if r["product"] else (r["brand"] if r["brand"] else r["key"]),
        axis=1
    )

    # 평균/등장일/최고순위
    agg = (d.groupby("key", as_index=False)
             .agg(mean_rank=("rank","mean"),
                  days=("rank","count"),
                  best=("rank","min")))
    agg = agg[agg["days"]>=min_days].copy()
    top = (agg.merge(latest, on="key", how="left")
             .sort_values(["mean_rank","best"])
             .head(10))

    # 브랜드 점유율(노출수 기준)
    brand = d.copy()
    brand["brand"] = brand["brand"].fillna("기타")
    brand_share = (brand.groupby("brand", as_index=False)
                        .agg(count=("rank","count"))
                   ).sort_values("count", ascending=False)

    # 지난주 비교
    prev_top = pd.DataFrame()
    brand_delta = pd.DataFrame()
    in_cnt = out_cnt = 0
    in_avg = out_avg = 0.0
    if df_prev is not None and not df_prev.empty:
        p = df_prev[pd.to_numeric(df_prev["rank"], errors="coerce")<=topn].copy()
        # prev top10 산출(동일 기준)
        prev_latest = (p.sort_values("date")
                         .groupby("key", as_index=False)
                         .agg(product=("product","last")))
        prev_agg = (p.groupby("key", as_index=False)
                      .agg(mean_rank=("rank","mean"),
                           days=("rank","count"),
                           best=("rank","min")))
        prev_agg = prev_agg[prev_agg["days"]>=min_days]
        prev_top = (prev_agg.merge(prev_latest, on="key", how="left")
                           .sort_values(["mean_rank","best"])
                           .head(10))

        # 브랜드 점유율 증감
        prev_brand = p.copy()
        prev_brand["brand"] = prev_brand["brand"].fillna("기타")
        prev_share = (prev_brand.groupby("brand", as_index=False)
                                .agg(prev_count=("rank","count")))
        brand_delta = (brand_share.merge(prev_share, on="brand", how="outer")
                                  .fillna(0.0))
        brand_delta["delta"] = brand_delta["count"] - brand_delta["prev_count"]
        brand_delta = brand_delta.sort_values("delta", ascending=False)

        # IN / OUT (주간 집합 비교)
        cur_keys = set(d["key"].unique())
        prev_keys = set(p["key"].unique())
        in_cnt = len(cur_keys - prev_keys)
        out_cnt = len(prev_keys - cur_keys)

        # 일평균 인/아웃
        cur_by_day = d.groupby(d["date"].dt.date)["key"].apply(lambda s: set(s)).tolist()
        prev_by_day = p.groupby(p["date"].dt.date)["key"].apply(lambda s: set(s)).tolist()
        if prev_by_day:
            # 일자 수 보정(최대 7일)
            days = max(len(cur_by_day), 1)
            # 전일 대비 daily IN/OUT 평균(근사)
            daily_in = []
            for i in range(len(cur_by_day)):
                prev_set = prev_by_day[i-1] if i-1 < len(prev_by_day) and i-1>=0 else set()
                daily_in.append(len(cur_by_day[i]-prev_set))
            in_avg = round(sum(daily_in)/days, 2)
            daily_out = []
            for i in range(len(cur_by_day)):
                prev_set = prev_by_day[i-1] if i-1 < len(prev_by_day) and i-1>=0 else set()
                daily_out.append(len(prev_set-cur_by_day[i]))
            out_avg = round(sum(daily_out)/days, 2)

    # 신규 히어로/반짝
    # 과거 4주 데이터 집계(이번 주 시작일 기준 이전 28일)
    if not d.empty:
        d_start = d["date"].min().normalize()
        hist_start = d_start - timedelta(days=28)
        # 같은 폴더에서 과거 4주 로드
        folder = os.getenv("DATA_DIR", "./data/daily")
        hist_df = load_daily_csvs(folder, hist_start.strftime("%Y-%m-%d"), (d_start - timedelta(days=1)).strftime("%Y-%m-%d"), only_source=src)
        hist_keys = set(hist_df["key"].unique()) if not hist_df.empty else set()

        cur_keys = set(d["key"].unique())
        # 신규 히어로: 과거 4주 미등장 & 이번주 ≥3일 & 평균순위 상위
        heroes = agg.copy()
        heroes = heroes[~heroes["key"].isin(hist_keys)]
        heroes = heroes.sort_values(["mean_rank","best"]).head(10)
        res_heroes = (heroes.merge(latest, on="key", how="left")).head(5)

        # 반짝: 이번 주 등장 ≤2일 & 평균순위 상위(Top30)
        flash = agg[(agg["days"]<=2) & (agg["mean_rank"]<=30)].copy()
        res_flash = (flash.merge(latest, on="key", how="left")
                          .sort_values(["mean_rank","best"])
                          .head(5))
    else:
        res_heroes = pd.DataFrame(); res_flash = pd.DataFrame()

    # 평균 할인율/가격대
    avg_discount = None; avg_price = None
    if ("discount_rate" in d.columns and d["discount_rate"].notna().any()):
        dr = pd.to_numeric(d["discount_rate"], errors="coerce").dropna()
        if len(dr): avg_discount = round(float(dr.mean()), 2)
    elif ("orig_price" in d.columns and "price" in d.columns):
        op = pd.to_numeric(d["orig_price"], errors="coerce")
        sp = pd.to_numeric(d["price"], errors="coerce")
        valid = (~op.isna()) & (~sp.isna()) & (op>0)
        if valid.any():
            disc = (1 - (sp[valid]/op[valid]))*100
            avg_discount = round(float(disc.mean()), 2)
    if "price" in d.columns and d["price"].notna().any():
        pr = pd.to_numeric(d["price"], errors="coerce").dropna()
        if len(pr): avg_price = int(pr.median())

    # 제품군 점유율(룰 기반)
    def map_category(name:str) -> str:
        nm = (name or "").lower()
        for cat, pat in CATEGORY_RULES:
            if re.search(pat, nm, re.IGNORECASE): return cat
        return "기타"
    cat_df = d.copy()
    cat_df["__cat"] = cat_df["product"].map(map_category)
    cat_share = (cat_df.groupby("__cat", as_index=False)
                      .agg(count=("rank","count"))
                 ).rename(columns={"__cat":"category"}).sort_values("count", ascending=False)

    # 키워드 점유율(간단 토큰화)
    toks = []
    for nm in d["product"].dropna().astype(str):
        # 괄호/기호 제거 후 공백 분리
        txt = re.sub(r"[\(\)\[\]【】{}·\-\+&/,:;!?\|\~]", " ", nm)
        for t in txt.split():
            t = t.strip().lower()
            if not t or t in STOPWORDS or len(t)<=1: continue
            toks.append(t)
    kw_counter = Counter(toks)
    kw_share = pd.DataFrame(kw_counter.most_common(20), columns=["keyword","count"])

    res.update({
        "top10": top, "brand_share": brand_share, "brand_share_delta": brand_delta,
        "in_count": in_cnt, "out_count": out_cnt,
        "in_avg_per_day": in_avg, "out_avg_per_day": out_avg,
        "new_heroes": res_heroes, "flash_items": res_flash,
        "avg_discount": avg_discount, "avg_price": avg_price,
        "cat_share": cat_share, "keyword_share": kw_share,
        "prev_top": prev_top
    })
    return res

def arrow(delta: float) -> str:
    if delta is None or (isinstance(delta,float) and math.isnan(delta)): return "—"
    d = int(round(delta))
    if d > 0: return f"▼{abs(d)}"
    if d < 0: return f"▲{abs(d)}"
    return "—"

def fmt_top10_lines(df_top: pd.DataFrame, prev_top: pd.DataFrame|None=None) -> list[str]:
    if df_top is None or df_top.empty:
        return ["데이터 없음"]
    prev_map = {}
    if isinstance(prev_top, pd.DataFrame) and not prev_top.empty:
        prev_map = dict(zip(prev_top["key"], prev_top["mean_rank"]))
    lines = []
    for i,r in enumerate(df_top.itertuples(),1):
        prev = prev_map.get(getattr(r,"key"))
        diff = None if prev is None else (getattr(r,"mean_rank") - prev)
        nm = getattr(r,"product") or getattr(r,"key")
        url = getattr(r,"url") or ""
        name = f"<{url}|{nm}>" if url else nm
        lines.append(f"{i}. {name} (등장 {int(r.days)}일) {arrow(diff)}")
    return lines

def fmt_brand_lines(df_brand: pd.DataFrame, df_delta: pd.DataFrame|None=None, k:int=12) -> list[str]:
    if df_brand is None or df_brand.empty:
        return ["데이터 없음"]
    if df_delta is None or df_delta.empty:
        return [f"{r.brand} {int(r.count)}개" for r in df_brand.head(k).itertuples()]
    m = df_delta.set_index("brand").to_dict().get("delta", {})
    out = []
    for r in df_brand.head(k).itertuples():
        delta = m.get(r.brand, 0)
        sign = "▲" if delta>0 else ("▼" if delta<0 else "—")
        out.append(f"{r.brand} {int(r.count)}개 {sign}{abs(int(delta)) if delta!=0 else ''}".rstrip())
    return out

def summarize_extras(res: dict) -> list[str]:
    lines = []
    # 인앤아웃
    lines.append(f"인앤아웃(주간): IN {res['in_count']}개 / OUT {res['out_count']}개 (일평균 IN {res['in_avg_per_day']} / OUT {res['out_avg_per_day']})")
    # 신규 히어로
    if not res["new_heroes"].empty:
        items = [f"<{u}|{n}>" if u else n for n,u in res["new_heroes"][["product","url"]].head(5).itertuples(index=False)]
        lines.append("신규 히어로: " + ", ".join(items))
    # 반짝
    if not res["flash_items"].empty:
        items = [f"<{u}|{n}>" if u else n for n,u in res["flash_items"][["product","url"]].head(5).itertuples(index=False)]
        lines.append("반짝 아이템: " + ", ".join(items))
    # 할인/가격
    if res["avg_discount"] is not None:
        lines.append(f"평균 할인율: {res['avg_discount']:.2f}%")
    if res["avg_price"] is not None:
        lines.append(f"중위가격: {res['avg_price']}")
    return lines

def run_weekly(data_dir:str, src:str, min_days:int=3) -> dict:
    rng = week_window_by_data(data_dir, src)
    if not rng:
        return {"range":"데이터 없음(소스 데이터 미발견)",
                "top10_lines":["데이터 없음"], "brand_lines":["데이터 없음"], "extra_lines":[]}
    start, end = rng
    df_cur = load_daily_csvs(data_dir, start, end, only_source=src)
    # 지난 주
    prev_start = (pd.to_datetime(start) - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_end   = (pd.to_datetime(end)   - timedelta(days=7)).strftime("%Y-%m-%d")
    df_prev = load_daily_csvs(data_dir, prev_start, prev_end, only_source=src)

    res = aggregate_week(df_cur, df_prev, src, min_days=min_days)
    top_lines = fmt_top10_lines(res["top10"], res["prev_top"])
    brand_lines = fmt_brand_lines(res["brand_share"], res["brand_share_delta"])
    extra_lines = summarize_extras(res)

    # 트렌드(선택 출력용): 상위 5개만 간단히 텍스트화
    trend = {
        "category_top5": res["cat_share"].head(5).to_dict(orient="records") if not res["cat_share"].empty else [],
        "keyword_top10": res["keyword_share"].head(10).to_dict(orient="records") if not res["keyword_share"].empty else [],
    }

    return {"range": f"{start}~{end}",
            "top10_lines": top_lines,
            "brand_lines": brand_lines,
            "extra_lines": extra_lines,
            "trend": trend}
