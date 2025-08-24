# weekly_report.py
# -*- coding: utf-8 -*-
"""
일간 CSV만으로 주간 리포트를 만드는 단일 스크립트(전문).
- 파일명에서 소스/날짜를 자동 추론
- 컬럼명이 제각각이어도 표준화
- URL에서 키 추출(goodsNo/prdtNo/asin/product_code/pdNo)
- 소스별 "데이터 최신일 기준"으로 주간(월~일, KST) 범위 산출
환경변수:
  DATA_DIR  : 일간 CSV 폴더 (기본 ./data/daily)
  MIN_DAYS  : 주간 Top10 최소 등장일 기준 (기본 3) — 데이터가 하루뿐이면 1로 낮추세요.
"""
import os, re, glob, json
import pandas as pd
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

SOURCE_INFO = {
    'oy_kor':    {'match': ['올리브영_랭킹', 'oliveyoung_kor'],          'topn':100},
    'oy_global': {'match': ['올리브영글로벌', 'oliveyoung_global'],       'topn':100},
    'amazon_us': {'match': ['아마존US', 'Amazon_US', 'amazonUS'],        'topn':100},
    'qoo10_jp':  {'match': ['큐텐재팬', 'Qoo10', 'qoo10'],               'topn':200},
    'daiso_kr':  {'match': ['다이소몰', 'Daiso'],                         'topn':200},
}

def infer_source_from_filename(fn: str) -> str|None:
    base = os.path.basename(fn)
    for src, info in SOURCE_INFO.items():
        for pat in info['match']:
            if pat in base:
                return src
    return None

def infer_date_from_filename(fn: str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(fn))
    return pd.to_datetime(m.group(1)) if m else pd.NaT

def read_csv_any(path: str) -> pd.DataFrame:
    for enc in ('utf-8-sig','cp949','utf-8','euc-kr'):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)  # 마지막 시도(에러 발생 시 트레이스)

def extract_key(src: str, url: str|None, row: dict) -> str|None:
    u = str(url or "")
    if src == 'oy_kor':
        m = re.search(r'goodsNo=([0-9A-Za-z\-]+)', u);  return m.group(1) if m else None
    if src == 'oy_global':
        m = re.search(r'(?:productId|prdtNo)=([0-9A-Za-z\-]+)', u);  return m.group(1) if m else None
    if src == 'amazon_us':
        if 'asin' in row and pd.notna(row['asin']):
            return str(row['asin']).upper()
        m = re.search(r'/([A-Z0-9]{10})(?:[/?#]|$)', u.upper());   return m.group(1) if m else None
    if src == 'qoo10_jp':
        if 'product_code' in row and pd.notna(row['product_code']):
            return str(row['product_code'])
        m = re.search(r'/(\d{6,})', u);                              return m.group(1) if m else None
    if src == 'daiso_kr':
        m = re.search(r'pdNo=([0-9A-Za-z\-]+)', u);                 return m.group(1) if m else None
    return None

def load_unified(data_dir: str, only_source: str|None=None) -> pd.DataFrame:
    paths = glob.glob(os.path.join(data_dir, '*.csv'))
    rows = []
    for p in paths:
        src = infer_source_from_filename(p)
        if only_source and src != only_source:
            continue
        try:
            df = read_csv_any(p)
        except Exception:
            continue
        # 날짜
        if 'date' in df.columns:
            dates = pd.to_datetime(df['date'])
        else:
            dates = pd.Series([infer_date_from_filename(p)]*len(df))
        if 'rank' not in df.columns:
            continue

        # 제품명/브랜드/URL 컬럼 매핑
        prod_col = next((c for c in ('product','name','product_name','title') if c in df.columns), None)
        brand_col = 'brand' if 'brand' in df.columns else None
        url_col   = 'url'   if 'url'   in df.columns else None

        for i, r in df.iterrows():
            rec = {
                'source': src,
                'date': pd.to_datetime(dates.iloc[i]),
                'rank': pd.to_numeric(r['rank'], errors='coerce'),
                'product': (str(r[prod_col]) if prod_col else None),
                'brand': (str(r[brand_col]) if brand_col else None) if brand_col else None,
                'url': r[url_col] if url_col else None,
            }
            rec['key'] = extract_key(src, rec['url'], r.to_dict())
            rows.append(rec)

    ud = pd.DataFrame(rows, columns=['source','date','rank','product','brand','url','key'])
    ud = ud.dropna(subset=['source','date','rank','key'])
    return ud

def week_range_for_source(ud: pd.DataFrame, src: str):
    dts = ud.loc[ud['source'].eq(src), 'date']
    if dts.empty:
        return None
    last = pd.to_datetime(dts.max())
    # 월(0)~일(6), 마지막 날짜가 포함된 주의 월~일
    end = last + pd.Timedelta(days=(6 - last.weekday()))
    start = end - pd.Timedelta(days=6)
    return start.normalize(), end.normalize()

def aggregate_source(ud: pd.DataFrame, src: str, min_days: int) -> dict:
    result = {'top10_lines':['데이터 없음'], 'brand_lines':['데이터 없음'], 'range':'데이터 없음'}
    rng = week_range_for_source(ud, src)
    if rng is None:
        return result
    start, end = rng
    topn = SOURCE_INFO[src]['topn']
    d = ud[(ud['source'].eq(src)) & (ud['date']>=start) & (ud['date']<=end) & (ud['rank']<=topn)].copy()
    if d.empty:
        result['range'] = f"{start.date()}~{end.date()}"
        return result

    # 평균순위/등장일
    agg = (d.groupby('key', as_index=False)
             .agg(mean_rank=('rank','mean'),
                  days=('rank','count')))
    agg = agg[agg['days']>=min_days].copy()

    latest = (d.sort_values('date')
                .groupby('key', as_index=False)
                .agg(product=('product','last'),
                     brand=('brand','last'),
                     url=('url','last')))

    top = (agg.merge(latest, on='key', how='left')
             .sort_values('mean_rank')
             .head(10))

    # 출력 포맷
    top_lines = []
    for i, r in enumerate(top.itertuples(), 1):
        nm = getattr(r, 'product') or getattr(r, 'key')
        url = getattr(r, 'url') or ''
        txt = f"{i}. {nm} (등장 {int(getattr(r,'days'))}일)"
        top_lines.append(txt)

    brand_d = d.copy()
    brand_d['brand'] = brand_d['brand'].fillna('기타')
    brand = (brand_d.groupby('brand')
                    .size()
                    .sort_values(ascending=False)
                    .head(12))
    brand_lines = [f"{b} {int(c)}개" for b,c in brand.items()]

    result['top10_lines'] = top_lines if top_lines else ['데이터 없음']
    result['brand_lines'] = brand_lines if brand_lines else ['데이터 없음']
    result['range'] = f"{start.date()}~{end.date()}"
    return result

def main():
    data_dir = os.getenv('DATA_DIR', './data/daily')
    min_days = int(os.getenv('MIN_DAYS', '3'))
    ud = load_unified(data_dir)

    out = {}
    for src in SOURCE_INFO.keys():
        out[src] = aggregate_source(ud, src, min_days=min_days)

    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
