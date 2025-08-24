# scripts/weekly_report_v2.py
# -*- coding: utf-8 -*-
import os, re, glob, json
import pandas as pd
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

SRC_INFO = {
    'oy_kor':    {'topn':100, 'hints':['올리브영_랭킹','oliveyoung_kor','oy_kor']},
    'oy_global': {'topn':100, 'hints':['올리브영글로벌','oliveyoung_global','oy_global']},
    'amazon_us': {'topn':100, 'hints':['아마존US','amazon_us','amazonUS']},
    'qoo10_jp':  {'topn':200, 'hints':['큐텐재팬','qoo10','Qoo10']},
    'daiso_kr':  {'topn':200, 'hints':['다이소몰','daiso']},
}
ALL_SRCS = list(SRC_INFO.keys())

COLS = {
    'rank':        ['rank','순위','ranking','랭킹'],
    'product':     ['product','제품명','name','상품명','title'],
    'brand':       ['brand','브랜드'],
    'url':         ['url','링크','product_url','link'],
    'date':        ['date','날짜','수집일','crawl_date'],
    'goodsNo':     ['goodsNo','goods_no','goodsno','상품번호','상품코드'],
    'productId':   ['productId','product_id','prdtNo','상품ID','상품아이디','상품코드'],
    'asin':        ['asin','ASIN'],
    'product_code':['product_code','productCode','상품코드','item_code'],
    'pdNo':        ['pdNo','pdno','상품번호','상품코드'],
}

def pick(df, names):
    for c in names:
        if c in df.columns: return c
    return None

def infer_source(path:str):
    base = os.path.basename(path).lower()
    parent = os.path.basename(os.path.dirname(path)).lower()
    for src, info in SRC_INFO.items():
        for h in info['hints']:
            h = h.lower()
            if h in base or h in parent:
                return src
    return None

def infer_date_from_filename(fn:str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(fn))
    return pd.to_datetime(m.group(1)) if m else pd.NaT

def read_csv_any(path:str)->pd.DataFrame:
    for enc in ('utf-8-sig','cp949','utf-8','euc-kr'):
        try: return pd.read_csv(path, encoding=enc)
        except Exception: pass
    return pd.read_csv(path)

def extract_key(src:str, row, url:str|None):
    u = str(url or "")
    if src == 'oy_kor':
        for c in COLS['goodsNo']:
            if c in row and pd.notna(row[c]): return str(row[c]).strip()
        m = re.search(r'goodsNo=([0-9A-Za-z\-]+)', u);  return m.group(1) if m else None
    if src == 'oy_global':
        for c in COLS['productId']:
            if c in row and pd.notna(row[c]): return str(row[c]).strip()
        m = re.search(r'(?:productId|prdtNo)=([0-9A-Za-z\-]+)', u); return m.group(1) if m else None
    if src == 'amazon_us':
        for c in COLS['asin']:
            if c in row and pd.notna(row[c]): return str(row[c]).strip().upper()
        m = re.search(r'/([A-Z0-9]{10})(?:[/?#]|$)', u.upper()); return m.group(1) if m else None
    if src == 'qoo10_jp':
        for c in COLS['product_code']:
            if c in row and pd.notna(row[c]): return str(row[c]).strip()
        m = re.search(r'product_code=([0-9A-Za-z\-]+)', u)
        if m: return m.group(1)
        m2 = re.search(r'/(\d{6,})', u)
        return m2.group(1) if m2 else None
    if src == 'daiso_kr':
        for c in COLS['pdNo']:
            if c in row and pd.notna(row[c]): return str(row[c]).strip()
        m = re.search(r'pdNo=([0-9A-Za-z\-]+)', u); return m.group(1) if m else None
    return None

def load_unified(data_dir:str)->pd.DataFrame:
    paths = glob.glob(os.path.join(data_dir,'**','*.csv'), recursive=True)
    rows = []
    for p in paths:
        src = infer_source(p)
        if not src: 
            continue
        try:
            df = read_csv_any(p)
        except Exception:
            continue
        date_col = pick(df, COLS['date'])
        if date_col:
            dates = pd.to_datetime(df[date_col], errors='coerce')
        else:
            dates = pd.Series([infer_date_from_filename(p)]*len(df))
        rank_col = pick(df, COLS['rank'])
        if not rank_col: 
            continue
        prod_col = pick(df, COLS['product'])
        brand_col = pick(df, COLS['brand'])
        url_col   = pick(df, COLS['url'])
        for i, r in df.iterrows():
            rec = {
                'source': src,
                'date': dates.iloc[i],
                'rank': pd.to_numeric(r.get(rank_col), errors='coerce'),
                'product': str(r.get(prod_col)) if prod_col else None,
                'brand': str(r.get(brand_col)) if brand_col else None,
                'url': r.get(url_col) if url_col else None,
            }
            rec['key'] = extract_key(src, r, rec['url'])
            rows.append(rec)
    ud = pd.DataFrame(rows, columns=['source','date','rank','product','brand','url','key'])
    ud = ud.dropna(subset=['source','date','rank','key'])
    return ud

def week_range_for_source(ud:pd.DataFrame, src:str):
    dts = ud.loc[ud['source'].eq(src), 'date']
    if dts.empty: return None
    last = pd.to_datetime(dts.max())
    end = last + pd.Timedelta(days=(6 - last.weekday()))
    start = end - pd.Timedelta(days=6)
    return start.normalize(), end.normalize()

def aggregate_source(ud:pd.DataFrame, src:str, min_days:int)->dict:
    out = {'top10_lines':['데이터 없음'], 'brand_lines':['데이터 없음'], 'range':'데이터 없음'}
    rng = week_range_for_source(ud, src)
    if not rng: return out
    start, end = rng
    topn = SRC_INFO[src]['topn']
    d = ud[(ud['source'].eq(src)) & (ud['date']>=start) & (ud['date']<=end) & (ud['rank']<=topn)].copy()
    if d.empty:
        out['range'] = f"{start.date()}~{end.date()}"
        return out
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
    top_lines = []
    for i, r in enumerate(top.itertuples(), 1):
        nm = getattr(r,'product') or getattr(r,'key')
        url = getattr(r,'url') or ''
        if url: nm = f"<{url}|{nm}>"
        top_lines.append(f"{i}. {nm} (등장 {int(getattr(r,'days'))}일)")
    brand_d = d.copy()
    brand_d['brand'] = brand_d['brand'].fillna('기타')
    brand = (brand_d.groupby('brand').size()
             .sort_values(ascending=False).head(12))
    brand_lines = [f"{b} {int(c)}개" for b,c in brand.items()]
    out['top10_lines'] = top_lines if top_lines else ['데이터 없음']
    out['brand_lines'] = brand_lines if brand_lines else ['데이터 없음']
    out['range'] = f"{start.date()}~{end.date()}"
    return out

def main():
    data_dir = os.getenv('DATA_DIR','./data/daily')
    min_days = int(os.getenv('MIN_DAYS','3'))
    ud = load_unified(data_dir)
    result = {}
    for src in ALL_SRCS:
        result[src] = aggregate_source(ud, src, min_days=min_days)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
