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

# 한글/영문 컬럼 동의어(제품명 매우 넓게)
COLS = {
    'rank':        ['rank','순위','ranking','랭킹'],
    'product':     ['product','제품명','상품명','name','title','goods_name','goodsNm','prdNm','prdtName','displayName','itemNm','상품'],
    'brand':       ['brand','브랜드','brand_name','brandNm','브랜드명'],
    'url':         ['url','링크','product_url','link','detail_url'],
    'date':        ['date','날짜','수집일','crawl_date','created_at'],
    'price':       ['price','가격','sale_price','selling_price'],
    'orig_price':  ['orig_price','정가','original_price','소비자가','list_price'],
    'discount':    ['discount_rate','할인율','discount','discountPercent'],
    # ID 후보
    'goodsNo':     ['goodsNo','goods_no','goodsno','상품번호','상품코드'],
    'productId':   ['productId','product_id','prdtNo','상품ID','상품아이디','상품코드'],
    'asin':        ['asin','ASIN'],
    'product_code':['product_code','productCode','상품코드','item_code'],
    'pdNo':        ['pdNo','pdno','상품번호','상품코드'],
}

PROMO_PREFIX_PAT = re.compile(r"^(올영픽|올영 픽|[가-힣A-Za-z0-9]+특가|[A-Za-z]+ ?Pick)\s*[-\s·]*", re.IGNORECASE)

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

STOPWORDS = set("""
의 가 이 은 는 을 를 에 에서 으로 도 과 와 및 ( ) , . : · - & x X + the and or for of with
세트 1+1 2+1 10개입 20매 30g 50ml 100ml 200ml pack set
""".split())

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

def strip_promo_prefix(name: str) -> str:
    if not name: return name
    return PROMO_PREFIX_PAT.sub("", str(name)).strip()

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
        m2 = re.search(r'/(\d{6,})', u)  # URL 말단 숫자
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

        # 날짜
        date_col = pick(df, COLS['date'])
        if date_col:
            dates = pd.to_datetime(df[date_col], errors='coerce')
        else:
            dates = pd.Series([infer_date_from_filename(p)]*len(df))

        # 순위
        rank_col = pick(df, COLS['rank'])
        if not rank_col: 
            continue

        # 이름/브랜드/URL/가격/할인
        prod_col = pick(df, COLS['product'])
        brand_col = pick(df, COLS['brand'])
        url_col   = pick(df, COLS['url'])
        price_col = pick(df, COLS['price'])
        orig_col  = pick(df, COLS['orig_price'])
        disc_col  = pick(df, COLS['discount'])

        for i, r in df.iterrows():
            nm = (str(r.get(prod_col)) if prod_col else None)
            nm = strip_promo_prefix(nm) if nm else nm
            rec = {
                'source': src,
                'date': dates.iloc[i],
                'rank': pd.to_numeric(r.get(rank_col), errors='coerce'),
                'product': nm,
                'brand': str(r.get(brand_col)) if brand_col else None,
                'url': r.get(url_col) if url_col else None,
                'price': pd.to_numeric(r.get(price_col), errors='coerce') if price_col else None,
                'orig_price': pd.to_numeric(r.get(orig_col), errors='coerce') if orig_col else None,
                'discount_rate': pd.to_numeric(r.get(disc_col), errors='coerce') if disc_col else None,
            }
            rec['key'] = extract_key(src, r, rec['url'])
            rows.append(rec)

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

def _arrow(delta: float) -> str:
    if delta is None or (isinstance(delta,float) and math.isnan(delta)): return "—"
    d = int(round(delta))
    if d > 0: return f"▼{abs(d)}"
    if d < 0: return f"▲{abs(d)}"
    return "—"

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

    # 지난 주/과거 4주
    prev_start = start - pd.Timedelta(days=7); prev_end = end - pd.Timedelta(days=7)
    prev = ud[(ud['source'].eq(src)) & (ud['date']>=prev_start) & (ud['date']<=prev_end) & (ud['rank']<=topn)].copy()

    hist_start = start - pd.Timedelta(days=28); hist_end = start - pd.Timedelta(days=1)
    hist = ud[(ud['source'].eq(src)) & (ud['date']>=hist_start) & (ud['date']<=hist_end) & (ud['rank']<=topn)].copy()

    # 최신 속성
    latest = (cur.sort_values('date')
                .groupby('key', as_index=False)
                .agg(product=('product','last'),
                     brand=('brand','last'),
                     url=('url','last')))

    # 평균/등장일
    agg = (cur.groupby('key', as_index=False)
             .agg(mean_rank=('rank','mean'),
                  days=('rank','count'),
                  best=('rank','min')))
    agg = agg[agg['days']>=min_days].copy()

    prev_map = None
    if not prev.empty:
        prev_agg = (prev.groupby('key', as_index=False)
                      .agg(mean_rank=('rank','mean')))
        prev_map = dict(zip(prev_agg['key'], prev_agg['mean_rank']))

    top = (agg.merge(latest, on='key', how='left')
             .sort_values(['mean_rank','best'])
             .head(10))
    top_lines = []
    for i, r in enumerate(top.itertuples(), 1):
        prev_mean = None if prev_map is None else prev_map.get(getattr(r,'key'))
        diff = None if prev_mean is None else (getattr(r,'mean_rank') - prev_mean)
        nm = getattr(r,'product') or getattr(r,'brand') or getattr(r,'key')
        u  = getattr(r,'url') or ''
        label = f"<{u}|{nm}>" if u else nm
        top_lines.append(f"{i}. {label} (등장 {int(getattr(r,'days'))}일) {_arrow(diff)}")

    # 브랜드 점유율 + 증감
    brand_cur = cur.copy()
    brand_cur['brand'] = brand_cur['brand'].fillna('기타')
    b_now = (brand_cur.groupby('brand').size().reset_index(name='count'))
    if not prev.empty:
        brand_prev = prev.copy()
        brand_prev['brand'] = brand_prev['brand'].fillna('기타')
        b_prev = brand_prev.groupby('brand').size().reset_index(name='prev')
        b = b_now.merge(b_prev, on='brand', how='left').fillna(0.0)
        b['delta'] = b['count'] - b['prev']
    else:
        b = b_now.assign(delta=0)
    b = b.sort_values(['count','delta'], ascending=[False, False]).head(12)
    brand_lines = []
    for r in b.itertuples():
        sign = "▲" if r.delta>0 else ("▼" if r.delta<0 else "—")
        suffix = f" {sign}{abs(int(r.delta))}" if r.delta!=0 else " —"
        brand_lines.append(f"{r.brand} {int(r.count)}개{suffix}")

    # IN / OUT
    cur_keys = set(cur['key'].unique())
    prev_keys = set(prev['key'].unique()) if not prev.empty else set()
    in_cnt = len(cur_keys - prev_keys)
    out_cnt = len(prev_keys - cur_keys)

    # 일평균 IN/OUT(전일 대비 근사)
    cur_days = sorted(set(cur['date'].dt.date))
    prev_days = sorted(set(prev['date'].dt.date)) if not prev.empty else []
    def keys_by_day(df):
        return {d: set(df[df['date'].dt.date.eq(d)]['key']) for d in sorted(set(df['date'].dt.date))}
    cur_map = keys_by_day(cur)
    prev_map_days = keys_by_day(prev) if not prev.empty else {}
    daily_in = []; daily_out = []
    for i, d in enumerate(cur_days):
        y = cur_days[i-1] if i>0 else None
        prev_set = prev_map_days.get(y, set()) if y else set()
        cur_set = cur_map.get(d, set())
        daily_in.append(len(cur_set - prev_set))
        daily_out.append(len(prev_set - cur_set))
    in_avg = round(sum(daily_in)/max(len(cur_days),1), 2)
    out_avg = round(sum(daily_out)/max(len(cur_days),1), 2)

    # 신규 히어로 / 반짝
    hist_keys = set(hist['key'].unique()) if not hist.empty else set()
    heroes = (agg[~agg['key'].isin(hist_keys)]
                .merge(latest, on='key', how='left')
                .sort_values(['mean_rank','best'])
                .head(5))
    flash = (agg[(agg['days']<=2)]
                .merge(latest, on='key', how='left')
                .sort_values(['mean_rank','best'])
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
    if 'discount_rate' in cur and cur['discount_rate'].notna().any():
        avg_disc = round(float(cur['discount_rate'].dropna().mean()), 2)
    elif 'orig_price' in cur and 'price' in cur:
        op = cur['orig_price']; sp = cur['price']
        valid = (~op.isna()) & (~sp.isna()) & (op>0)
        if valid.any():
            avg_disc = round(float(((1 - sp[valid]/op[valid])*100).mean()), 2)
    if 'price' in cur and cur['price'].notna().any():
        med_price = int(cur['price'].dropna().median())

    # 카테고리 점유율
    def map_cat(name:str)->str:
        nm = (name or "").lower()
        for cat, pat in CATEGORY_RULES:
            if re.search(pat, nm, re.IGNORECASE): return cat
        return "기타"
    cats = cur.copy()
    cats['__cat'] = cats['product'].map(map_cat)
    cat_top5 = cats.groupby('__cat').size().sort_values(ascending=False).head(5)
    cat_pairs = [(c,int(n)) for c,n in cat_top5.items()]

    # 키워드 Top10
    toks = []
    for nm in cur['product'].dropna().astype(str):
        txt = re.sub(r"[\(\)\[\]{}·\-\+&/,:;!?\|~]", " ", nm)
        for t in txt.split():
            t = t.strip().lower()
            if not t or t in STOPWORDS or len(t)<=1: continue
            toks.append(t)
    kw = Counter(toks).most_common(10)

    # 결과
    res.update({
        'range': f"{start.date()}~{end.date()}",
        'top10_lines': top_lines,
        'brand_lines': brand_lines,
        'inout': f"IN {in_cnt} / OUT {out_cnt} (일평균 IN {in_avg} / OUT {out_avg})",
        'heroes': to_links(heroes),
        'flash': to_links(flash),
        'discount': avg_disc,
        'median_price': med_price,
        'cat_top5': [f"{c} {n}개" for c,n in cat_pairs],
        'kw_top10': [f"{k} {n}" for k,n in kw],
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
    lines.append("🏆 Top10 (평균 순위, raw 제품명)")
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
    # 결과 JSON
    print(json.dumps(result, ensure_ascii=False, indent=2))
    # 슬랙 메시지 본문도 파일로 저장(옵션)
    with open("weekly_slack_message.txt","w",encoding="utf-8") as f:
        f.write("\n\n— — —\n\n".join(slack_texts))

if __name__ == "__main__":
    main()
