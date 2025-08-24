# scripts/weekly_report_plus.py
# -*- coding: utf-8 -*-
import os, re, glob, json, math
import pandas as pd
from collections import Counter
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

# === 컬럼 동의어 (제품명은 raw_name 최우선, 그대로 표기) ===
COLS = {
    'rank':        ['rank','순위','ranking','랭킹'],
    'raw_name':    ['raw_name','raw','rawProduct','rawTitle'],
    'product':     ['product','제품명','상품명','name','title','goods_name','goodsNm',
                    'prdNm','prdtName','displayName','itemNm','상품','item_name','item'],
    'brand':       ['brand','브랜드','brand_name','brandNm','브랜드명'],
    'url':         ['url','링크','product_url','link','detail_url'],
    'date':        ['date','날짜','수집일','crawl_date','created_at'],
    'price':       ['price','가격','sale_price','selling_price'],
    'orig_price':  ['orig_price','정가','original_price','소비자가','list_price'],
    'discount':    ['discount_rate','할인율','discount','discountPercent'],
    'goodsNo':     ['goodsNo','goods_no','goodsno','상품번호','상품코드'],
    'productId':   ['productId','product_id','prdtNo','상품ID','상품아이디','상품코드'],
    'asin':        ['asin','ASIN'],
    'product_code':['product_code','productCode','상품코드','item_code'],
    'pdNo':        ['pdNo','pdno','상품번호','상품코드'],
}

STOPWORDS = set("""
의 가 이 은 는 을 를 에 에서 으로 도 과 와 및 ( ) , . : · - & x X + the and or for of with
세트 1+1 2+1 10개입 20매 30g 50ml 100ml 200ml pack set
""".split())

CATEGORY_RULES = [
    ("마스크팩", r"(마스크팩|팩|sheet\s*mask|mask\s*pack)"),
    ("선케어", r"(선크림|자외선|sun\s*cream|sunscreen|uv)"),
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

def pick(df, names):
    for c in names:
        if c in df.columns: return c
    return None

def pick_loose_product(df):
    rn = pick(df, COLS['raw_name'])
    if rn: return rn
    pn = pick(df, COLS['product'])
    if pn: return pn
    patt = re.compile(r"(product|name|title|상품|제품|품명|아이템)", re.IGNORECASE)
    for c in df.columns:
        if patt.search(str(c)): return c
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
        if not src: continue
        try:
            df = read_csv_any(p)
        except Exception:
            continue

        date_col = pick(df, COLS['date'])
        dates = pd.to_datetime(df[date_col], errors='coerce') if date_col else pd.Series([infer_date_from_filename(p)]*len(df))
        rank_col = pick(df, COLS['rank'])
        if not rank_col: continue

        prod_col = pick_loose_product(df)     # raw_name 우선, 그대로 사용
        brand_col = pick(df, COLS['brand'])
        url_col   = pick(df, COLS['url'])
        price_col = pick(df, COLS['price'])
        orig_col  = pick(df, COLS['orig_price'])
        disc_col  = pick(df, COLS['discount'])

        for i, r in df.iterrows():
            nm = str(r.get(prod_col)) if prod_col else None
            rows.append({
                'source': src,
                'date': dates.iloc[i],
                'rank': pd.to_numeric(r.get(rank_col), errors='coerce'),
                'product': nm,
                'brand': str(r.get(brand_col)) if brand_col else None,
                'url': r.get(url_col) if url_col else None,
                'price': pd.to_numeric(r.get(price_col), errors='coerce') if price_col else None,
                'orig_price': pd.to_numeric(r.get(orig_col), errors='coerce') if orig_col else None,
                'discount_rate': pd.to_numeric(r.get(disc_col), errors='coerce') if disc_col else None,
                'key': extract_key(src, r, r.get(url_col) if url_col else None),
            })

    ud = pd.DataFrame(rows, columns=['source','date','rank','product','brand','url','price','orig_price','discount_rate','key'])
    ud = ud.dropna(subset=['source','date','rank','key'])
    return ud

def week_range_for_source(ud:pd.DataFrame, src:str):
    dts = ud.loc[ud['source'].eq(src), 'date']
    if dts.empty: return None
    last = pd.to_datetime(dts.max())
    end = last + pd.Timedelta(days=(6 - last.weekday()))  # 그 주 일요일
    start = end - pd.Timedelta(days=6)                    # 그 주 월요일
    return start.normalize(), end.normalize()

def _arrow_points(diff: float) -> str:
    if diff is None or (isinstance(diff,float) and math.isnan(diff)): return "—"
    d = int(round(diff))
    if d > 0: return f"▲{d}"    # 점수 상승 = 개선
    if d < 0: return f"▼{abs(d)}"
    return "—"

def _day_set(df, day, topn):
    d = df[df['date'].dt.date.eq(day)].sort_values('rank').drop_duplicates('key')
    return set(d.head(topn)['key'])

def _inout_daily(cur, prev, topn, start):
    days = sorted(set(cur['date'].dt.date))
    if not days: return 0, 0, 0.0
    prev_last = _day_set(prev, (start - pd.Timedelta(days=1)).date(), topn) if not prev.empty else set()
    total_in = total_out = 0
    last_set = prev_last
    for d in days:
        cur_set = _day_set(cur, d, topn)
        enter = cur_set - last_set
        leave = last_set - cur_set
        total_in  += len(enter)
        total_out += len(leave)
        last_set = cur_set
    if total_in != total_out:
        m = max(total_in, total_out)
        total_in = total_out = m
    return total_in, total_out, round(total_in/len(days), 2)

def _weekly_points_table(df, topn):
    tmp = df.copy()
    tmp['__pts'] = topn + 1 - tmp['rank']
    return (tmp.groupby('key', as_index=False)
                .agg(points=('__pts','sum'),
                     days=('rank','count'),
                     best=('rank','min')))

def summarize_week(ud:pd.DataFrame, src:str, min_days:int=3):
    res = {
        'range': '데이터 없음',
        'top10_lines': ['데이터 없음'],
        'brand_lines': ['데이터 없음'],
        'inout': '',
        'heroes': [],
        'flash': [],
        'discount': None,
        'median_price': None,
        'cat_top5': [],
        'kw_top10': [],
    }
    rng = week_range_for_source(ud, src)
    if not rng: return res
    start, end = rng
    topn = SRC_INFO[src]['topn']

    cur = ud[(ud['source'].eq(src)) & (ud['date']>=start) & (ud['date']<=end) & (ud['rank']<=topn)].copy()
    if cur.empty:
        res['range'] = f"{start.date()}~{end.date()}"
        return res

    prev = ud[(ud['source'].eq(src)) &
              (ud['date']>=start - pd.Timedelta(days=7)) &
              (ud['date']<=end - pd.Timedelta(days=7)) &
              (ud['rank']<=topn)].copy()

    hist = ud[(ud['source'].eq(src)) &
              (ud['date']>=start - pd.Timedelta(days=28)) &
              (ud['date']<=start - pd.Timedelta(days=1)) &
              (ud['rank']<=topn)].copy()

    pts = _weekly_points_table(cur, topn)
    pts = pts[pts['days'] >= min_days]
    latest = (cur.sort_values('date')
                .groupby('key', as_index=False)
                .agg(product=('product','last'),
                     brand=('brand','last'),
                     url=('url','last')))
    prev_pts_map = {}
    if not prev.empty:
        prev_pts = _weekly_points_table(prev, topn)
        prev_pts_map = dict(zip(prev_pts['key'], prev_pts['points']))

    top = (pts.merge(latest, on='key', how='left')
             .sort_values(['points','days','best'], ascending=[False, False, True])
             .head(10))

    # === Top10 라인: '주간점수' 문구 제거, '유지' 용어 사용, 화살표는 점수 증감 ===
    top_lines = []
    for i, r in enumerate(top.itertuples(), 1):
        prev_p = prev_pts_map.get(getattr(r,'key'))
        diff = None if prev_p is None else (getattr(r,'points') - prev_p)
        nm = getattr(r,'product') or getattr(r,'brand') or getattr(r,'key')
        u  = getattr(r,'url') or ''
        label = f"<{u}|{nm}>" if u else nm
        top_lines.append(f"{i}. {label} (유지 {int(getattr(r,'days'))}일) {_arrow_points(diff)}")

    # === 브랜드 점유율: 전주 동일 윈도우 비교 ===
    b_now = (cur.assign(brand=cur['brand'].fillna('기타'))
               .groupby('brand').size().reset_index(name='count'))
    if not prev.empty:
        b_prev = (prev.assign(brand=prev['brand'].fillna('기타'))
                    .groupby('brand').size().reset_index(name='prev'))
    else:
        b_prev = pd.DataFrame(columns=['brand','prev'])
    b = b_now.merge(b_prev, on='brand', how='left').fillna(0.0)
    b['delta'] = b['count'] - b['prev']
    b = b.sort_values(['count','delta'], ascending=[False, False]).head(12)
    brand_lines = []
    for r in b.itertuples():
        sign = "—"
        if r.delta > 0: sign = f"▲{int(r.delta)}"
        elif r.delta < 0: sign = f"▼{abs(int(r.delta))}"
        brand_lines.append(f"{r.brand} {int(r.count)}개 {sign}")

    # === IN/OUT: 전일 대비 집합 기준 → 항상 동치 ===
    in_cnt, out_cnt, daily_avg = _inout_daily(cur, prev, topn, start)

    # 신규 히어로 / 반짝 (raw_name 그대로 표기)
    hist_keys = set(hist['key'].unique()) if not hist.empty else set()
    heroes = (pts[~pts['key'].isin(hist_keys)]
                .merge(latest, on='key', how='left')
                .sort_values(['points','days','best'], ascending=[False, False, True])
                .head(5))
    flash = (pts[(pts['days']<=2)]
                .merge(latest, on='key', how='left')
                .sort_values(['points','days','best'], ascending=[False, False, True])
                .head(5))

    def to_links(df):
        out = []
        for r in df.itertuples():
            nm = getattr(r,'product') or getattr(r,'brand') or getattr(r,'key')
            u  = getattr(r,'url') or ''
            out.append(f"<{u}|{nm}>" if u else nm)
        return out

    # 할인/가격
    avg_disc = None; med_price = None
    if cur['discount_rate'].notna().any():
        avg_disc = round(float(cur['discount_rate'].dropna().mean()), 2)
    elif 'orig_price' in cur and 'price' in cur:
        op = cur['orig_price']; sp = cur['price']
        valid = (~op.isna()) & (~sp.isna()) & (op>0)
        if valid.any():
            avg_disc = round(float(((1 - sp[valid]/op[valid])*100).mean()), 2)
    if cur['price'].notna().any():
        med_price = int(cur['price'].dropna().median())

    # 카테고리/키워드
    def map_cat(name:str)->str:
        nm = (name or "").lower()
        for cat, pat in CATEGORY_RULES:
            if re.search(pat, nm, re.IGNORECASE): return cat
        return "기타"
    cats = cur.copy()
    cats['__cat'] = cats['product'].map(map_cat)
    cat_top5 = cats.groupby('__cat').size().sort_values(ascending=False).head(5)
    cat_pairs = [f"{c} {int(n)}개" for c,n in cat_top5.items()]

    toks = []
    for nm in cur['product'].dropna().astype(str):
        txt = re.sub(r"[\(\)\[\]{}·\-\+&/,:;!?\|~]", " ", nm)
        for t in txt.split():
            t = t.strip().lower()
            if not t or t in STOPWORDS or len(t)<=1: continue
            toks.append(t)
    kw_top10 = [f"{k} {n}" for k,n in Counter(toks).most_common(10)]

    res.update({
        'range': f"{start.date()}~{end.date()}",
        'top10_lines': top_lines,
        'brand_lines': brand_lines,
        'inout': f"IN {in_cnt} / OUT {out_cnt} (일평균 {daily_avg})",
        'heroes': to_links(heroes),
        'flash': to_links(flash),
        'discount': avg_disc,
        'median_price': med_price,
        'cat_top5': cat_pairs,
        'kw_top10': kw_top10,
    })
    return res

def format_slack_block(src:str, s:dict)->str:
    title_map = {
        'oy_kor':   "올리브영 국내 Top100",
        'oy_global':"올리브영 글로벌 Top100",
        'amazon_us':"아마존 US Top100",
        'qoo10_jp': "큐텐 재팬 뷰티 Top200",
        'daiso_kr': "다이소몰 뷰티/위생 Top200",
    }
    lines = []
    lines.append(f"📊 주간 리포트 · {title_map.get(src, src)} ({s['range']})")
    lines.append("🏆 Top10 (raw 제품명)")
    lines.extend(s['top10_lines'] or ["데이터 없음"])
    lines.append("")
    lines.append("🍞 브랜드 점유율")
    lines.extend(s['brand_lines'] or ["데이터 없음"])
    lines.append("")
    lines.append(f"🔁 인앤아웃: {s['inout']}")
    if s['heroes']:
        lines.append("🆕 신규 히어로: " + ", ".join(s['heroes']))
    if s['flash']:
        lines.append("✨ 반짝 아이템: " + ", ".join(s['flash']))
    if s['discount'] is not None:
        lines.append(f"💰 평균 할인율: {s['discount']:.2f}%")
    if s['median_price'] is not None:
        lines.append(f"💵 중위가격: {s['median_price']}")
    if s['cat_top5']:
        lines.append("📈 카테고리 상위: " + " · ".join(s['cat_top5']))
    if s['kw_top10']:
        lines.append("#️⃣ 키워드 Top10: " + ", ".join(s['kw_top10']))
    return "\n".join(lines)

def main():
    data_dir = os.getenv('DATA_DIR','./data/daily')
    min_days = int(os.getenv('MIN_DAYS','3'))
    ud = load_unified(data_dir)
    result = {}
    slack_texts = []
    for src in ALL_SRCS:
        s = summarize_week(ud, src, min_days=min_days)
        result[src] = s
        slack_texts.append(format_slack_block(src, s))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    with open("weekly_slack_message.txt","w",encoding="utf-8") as f:
        f.write("\n\n— — —\n\n".join(slack_texts))

if __name__ == "__main__":
    main()
