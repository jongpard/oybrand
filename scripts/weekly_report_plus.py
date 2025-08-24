# scripts/weekly_report_plus.py
# -*- coding: utf-8 -*-
import os, re, glob, json, math, argparse
import pandas as pd
from collections import Counter
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

SRC_INFO = {
    'oy_kor':    {'topn':100, 'hints':['올리브영_랭킹','oliveyoung_kor','oy_kor'], 'currency':'KRW'},
    'oy_global': {'topn':100, 'hints':['올리브영글로벌','oliveyoung_global','oy_global'], 'currency':'KRW'},
    'amazon_us': {'topn':100, 'hints':['아마존US','amazon_us','amazonUS'],       'currency':'USD'},
    'qoo10_jp':  {'topn':200, 'hints':['큐텐재팬','qoo10','Qoo10'],               'currency':'JPY'},
    'daiso_kr':  {'topn':200, 'hints':['다이소몰','daiso'],                       'currency':'KRW'},
}
ALL_SRCS = list(SRC_INFO.keys())

# === 컬럼 동의어(제품명은 raw_name 최우선, 그대로 표기) ===
COLS = {
    'rank':        ['rank','순위','ranking','랭킹'],
    'raw_name':    ['raw_name','raw','rawProduct','rawTitle'],
    'product':     ['product','제품명','상품명','name','title','goods_name','goodsNm','prdNm','prdtName','displayName','itemNm','상품','item_name','item'],
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

# === 키워드 사전(제품형태/효능/마케팅) — % 점유 계산용 ===
KW_PRODUCT = {
    '패드': r'(패드|pad)',
    '마스크팩': r'(마스크팩|마스크|sheet\s*mask|mask\s*pack)',
    '앰플/세럼': r'(앰플|세럼|ampoule|serum)',
    '토너/스킨': r'(토너|스킨|toner)',
    '크림': r'(크림|cream|moisturizer)',
    '클렌저': r'(클렌징|클렌저|cleanser|워시|wash)',
    '선케어': r'(선크림|sunscreen|sun\s*cream|uv)',
    '립': r'(립|틴트|lip|tint|balm)',
}
KW_EFFICACY = {
    '진정': r'(진정|soothing|calming)',
    '보습': r'(보습|수분|hydration|moistur)',
    '미백/톤업': r'(미백|톤업|whiten|brighten)',
    '탄력/리프팅': r'(탄력|리프팅|firming|lifting|elastic)',
    '트러블/여드름': r'(트러블|여드름|acne|blemish)',
    '모공': r'(모공|pore)',
    '각질/필링': r'(각질|필링|peel|AHA|BHA|PHA)',
    '주름': r'(주름|wrinkle|anti[-\s]?aging)',
}
KW_MARKETING = {
    '기획/세트': r'(기획|세트|set|kit|bundle)',
    '1+1/증정': r'(1\+1|2\+1|증정|증량|덤)',
    '한정/NEW': r'(한정|리미티드|limited|NEW|new\b|신상)',
    '쿠폰/딜': r'(쿠폰|coupon|딜|deal|특가|sale|세일|event|프로모션|promotion)',
    'PICK/콜라보': r'(올영픽|PICK|pick|콜라보|collab)',
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

# ---------- 유틸 ----------
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
            if h in base or h in parent: return src
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

def fmt_money(v, src):
    if v is None or (isinstance(v,float) and (pd.isna(v) or math.isnan(v))): return None
    cur = SRC_INFO[src]['currency']
    if cur == 'USD':  return f"${v:,.0f}"
    if cur == 'JPY':  return f"¥{v:,.0f}"
    return f"₩{v:,.0f}"  # default KRW

# ---------- 프로모션 플래그 ----------
PROMO_RE = re.compile(
    r"(올영픽|PICK|pick|특가|기획|증정|세일|sale|event|행사|한정|리미티드|1\+1|2\+1|더블\s*기획|증량|쿠폰|coupon|deal|딜|gift|bundle|promotion)",
    re.IGNORECASE
)
def is_promo(name:str)->bool:
    n = (name or "")
    return bool(PROMO_RE.search(n))

# ---------- 인플루언서 동적 추출 ----------
INF_BLACKLIST = {'올영','올리브영','올영픽','원더','MD','에디터','브랜드','editor','brand','oliveyoung','스토어','공식','픽'}
def extract_influencers_dynamic(name:str):
    """raw 제품명에서 인플루언서 이름 후보를 동적으로 추출"""
    if not name: return set()
    text = str(name)
    found = set()

    # 1) "OO 픽 / OO PICK / OO Pick"
    for m in re.finditer(r'([가-힣A-Za-z]{2,20})\s*(?:의\s*)?(?:픽|PICK|Pick)\b', text):
        nm = m.group(1).strip()
        if nm not in INF_BLACKLIST and nm.lower() not in {x.lower() for x in INF_BLACKLIST}:
            found.add(nm)

    # 2) 콜라보/with/×/X
    for m in re.finditer(r'([가-힣A-Za-z]{2,20})\s*(?:×|x|X|with|콜라보|collab(?:oration)?)\s*([가-힣A-Za-z]{2,20})', text, re.IGNORECASE):
        for nm in (m.group(1), m.group(2)):
            nm = nm.strip()
            if nm not in INF_BLACKLIST and nm.lower() not in {x.lower() for x in INF_BLACKLIST}:
                # 숫자/단위/일반 키워드 제거
                if not re.search(r'\d|ml|g|pack|set|기획|특가|세트', nm, re.IGNORECASE):
                    found.add(nm)
    return found

# ---------- 베이스 테이블 ----------
def daily_topn_base(df, topn):
    # 날짜별 정렬→key 중복 제거→TopN 슬라이스
    d = (df.sort_values(['date','rank'])
            .drop_duplicates(['date','key'])
            .groupby(df['date'].dt.date, group_keys=False)
            .apply(lambda x: x.nsmallest(topn, 'rank')))
    d['day'] = d['date'].dt.date
    return d

def week_range_for_source(ud:pd.DataFrame, src:str):
    dts = ud.loc[ud['source'].eq(src), 'date']
    if dts.empty: return None
    last = pd.to_datetime(dts.max())
    end = last + pd.Timedelta(days=(6 - last.weekday()))  # 그 주 일요일
    start = end - pd.Timedelta(days=6)                    # 그 주 월요일
    return start.normalize(), end.normalize()

def _arrow_rank(diff_rank: float) -> str:
    # diff_rank = prev_mean - cur_mean (개선이면 양수)
    if diff_rank is None or (isinstance(diff_rank,float) and math.isnan(diff_rank)): return "—"
    d = int(round(diff_rank))
    if d > 0:  return f"↑{d}"
    if d < 0:  return f"↓{abs(d)}"
    return "—"

# ---------- 로딩 ----------
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
            br = str(r.get(brand_col)) if brand_col else None
            # 아마존 브랜드 정규화: 'Amazon' → 'Amazon Basics'
            if src == 'amazon_us' and br:
                if br.strip().lower() == 'amazon':
                    br = 'Amazon Basics'

            rows.append({
                'source': src,
                'date': dates.iloc[i],
                'rank': pd.to_numeric(r.get(rank_col), errors='coerce'),
                'product': nm,
                'brand': br,
                'url': r.get(url_col) if url_col else None,
                'price': pd.to_numeric(r.get(price_col), errors='coerce') if price_col else None,
                'orig_price': pd.to_numeric(r.get(orig_col), errors='coerce') if orig_col else None,
                'discount_rate': pd.to_numeric(r.get(disc_col), errors='coerce') if disc_col else None,
                'key': extract_key(src, r, r.get(url_col) if url_col else None),
                'promo': is_promo(nm),
                'infl': list(extract_influencers_dynamic(nm)),
            })

    ud = pd.DataFrame(rows, columns=[
        'source','date','rank','product','brand','url',
        'price','orig_price','discount_rate','key','promo','infl'
    ])
    ud = ud.dropna(subset=['source','date','rank','key'])
    return ud

# ---------- 집계 ----------
def summarize_week(ud:pd.DataFrame, src:str, min_days:int=3):
    res = {
        'range': '데이터 없음',
        'top10_lines': ['데이터 없음'],
        'brand_lines': ['데이터 없음'],
        'inout': '',
        'heroes': [],
        'flash': [],
        'discount_all': None,
        'discount_promo': None,
        'discount_nonpromo': None,
        'discount_delta_same': None,
        'median_price': None,
        'cat_top5': [],
        'kw_lines': [],     # 키워드(제품형태/효능/마케팅%) + 인플루언서 이름
        'insights': [],
        'stats': {},
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

    # 하루 기준 TopN 베이스
    cur_base  = daily_topn_base(cur, topn)
    prev_base = daily_topn_base(prev, topn)

    # --- 주간 테이블(점수/평균순위/유지일수) ---
    tmp = cur_base.copy()
    tmp['__pts'] = topn + 1 - tmp['rank']
    pts = (tmp.groupby('key', as_index=False)
              .agg(points=('__pts','sum'),
                   days=('rank','count'),
                   best=('rank','min'),
                   mean_rank=('rank','mean')))
    pts = pts[pts['days'] >= min_days]

    latest = (cur_base.sort_values('date')
                .groupby('key', as_index=False)
                .agg(product=('product','last'),
                     brand=('brand','last'),
                     url=('url','last')))

    prev_tbl = None; prev_mean_map = {}; prev_days_map = {}
    if not prev_base.empty:
        ptmp = prev_base.copy(); ptmp['__pts'] = topn + 1 - ptmp['rank']
        prev_tbl = (ptmp.groupby('key', as_index=False)
                        .agg(points=('__pts','sum'),
                             days=('rank','count'),
                             best=('rank','min'),
                             mean_rank=('rank','mean')))
        prev_mean_map = dict(zip(prev_tbl['key'], prev_tbl['mean_rank']))
        prev_days_map = dict(zip(prev_tbl['key'], prev_tbl['days']))

    # 정렬: 점수↓ → 유지일수↓ → 최고순위↑
    top = (pts.merge(latest, on='key', how='left')
             .sort_values(['points','days','best'], ascending=[False, False, True])
             .head(10))

    # Top10: (유지 n일 · 평균 xx.x위) (NEW/↑/↓/—)
    top_lines = []
    for i, r in enumerate(top.itertuples(), 1):
        key = getattr(r,'key')
        nm = getattr(r,'product') or getattr(r,'brand') or key
        u  = getattr(r,'url') or ''
        label = f"<{u}|{nm}>" if u else nm

        cur_mean  = getattr(r,'mean_rank')
        prev_mean = prev_mean_map.get(key)
        prev_days = prev_days_map.get(key, 0)
        delta_txt = "NEW" if (prev_mean is None or prev_days < min_days) else _arrow_rank(prev_mean - cur_mean)
        mean_txt = f"{round(float(cur_mean),1)}위" if cur_mean is not None else "-"

        top_lines.append(f"{i}. {label} (유지 {int(getattr(r,'days'))}일 · 평균 {mean_txt}) ({delta_txt})")

    # 브랜드 "개수/일" 비교
    def brand_daily_avg(base):
        return (base.groupby(['day','brand']).size()
                     .groupby('brand').mean().reset_index(name='per_day'))
    b_now = brand_daily_avg(cur_base).rename(columns={'per_day':'now'})
    b_prev = brand_daily_avg(prev_base).rename(columns={'per_day':'prev'}) if not prev_base.empty else pd.DataFrame(columns=['brand','prev'])
    b = (b_now.merge(b_prev, on='brand', how='left').fillna(0.0)
              .assign(delta=lambda x: x['now'] - x['prev'])
              .sort_values(['now','delta'], ascending=[False, False]).head(12))
    brand_lines = []
    for r in b.itertuples():
        if r.delta > 0:  sign = f"↑{round(r.delta,1)}"
        elif r.delta < 0: sign = f"↓{abs(round(r.delta,1))}"
        else:            sign = "—"
        brand_lines.append(f"{r.brand} {round(r.now,1)}개/일 ({sign})")

    # IN/OUT: 비교가능한 날만 계산 → 단일 값
    days = sorted(cur_base['day'].unique())
    prev_days_set = set(prev_base['day'].unique())
    total_in = total_out = 0; valid = 0
    for d in days:
        pd_ = pd.to_datetime(d) - pd.Timedelta(days=1)
        if pd_.date() not in prev_days_set: continue
        cur_set  = set(cur_base.loc[cur_base['day'].eq(d), 'key'])
        prev_set = set(prev_base.loc[prev_base['day'].eq(pd_.date()), 'key'])
        total_in  += len(cur_set - prev_set)
        total_out += len(prev_set - cur_set)
        valid += 1
    swaps = max(total_in, total_out)
    inout_text = "비교 기준 없음" if valid == 0 else f"{swaps} (일평균 {round(swaps/valid,2)} · {valid}/{len(days)}일 비교)"

    # 신규 히어로 / 반짝
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

    # 가격/할인: 상품별 주간 통계 → 중앙/평균
    wk = (cur_base.groupby('key')
            .agg(price_med=('price','median'),
                 disc_avg=('discount_rate','mean'))
            .reset_index())
    med_price = int(wk['price_med'].dropna().median()) if wk['price_med'].notna().any() else None

    # 프로모션 vs 일반
    promo_base = cur_base[cur_base['promo']==True]
    non_base   = cur_base[cur_base['promo']!=True]
    def _mean_disc(df):
        return round(float(df['discount_rate'].dropna().mean()),2) if df['discount_rate'].notna().any() else None
    disc_all   = _mean_disc(cur_base)
    disc_promo = _mean_disc(promo_base)
    disc_non   = _mean_disc(non_base)

    # 동일 상품의 프로모션 有/無 차이
    both = (cur_base.groupby(['key','promo'])['discount_rate']
                   .mean().reset_index().pivot(index='key', columns='promo', values='discount_rate').dropna())
    disc_delta_same = None
    if not both.empty:
        both['diff'] = both.get(True, pd.Series()) - both.get(False, pd.Series())
        if both['diff'].notna().any():
            disc_delta_same = round(float(both['diff'].mean()),2)

    # 카테고리 상위
    def map_cat(name:str)->str:
        nm = (name or "").lower()
        for cat, pat in CATEGORY_RULES:
            if re.search(pat, nm, re.IGNORECASE): return cat
        return "기타"
    cats = cur_base.copy()
    cats['__cat'] = cats['product'].map(map_cat)
    cat_top5 = cats.groupby('__cat').size().sort_values(ascending=False).head(5)
    cat_pairs = [f"{c} {int(n)}개" for c,n in cat_top5.items()]

    # 키워드(제품형태/효능/마케팅 %) + 인플루언서 이름 리스트
    def bucket_share(base, rules):
        cnt = Counter(); total_hits = 0
        for nm in base['product'].dropna().astype(str):
            for label, pat in rules.items():
                if re.search(pat, nm, re.IGNORECASE):
                    cnt[label] += 1; total_hits += 1
        if total_hits == 0: return []
        return [f"{k} {round(v*100/total_hits,1)}%" for k,v in cnt.most_common(5)]
    # 인플루언서 이름 수집
    infl_cnt = Counter()
    for names in cur_base['infl'].dropna():
        for n in names:
            infl_cnt[n] += 1
    infl_names = [n for n,_ in infl_cnt.most_common(8)]

    kw_lines = []
    p_items = bucket_share(cur_base, KW_PRODUCT)
    e_items = bucket_share(cur_base, KW_EFFICACY)
    m_items = bucket_share(cur_base, KW_MARKETING)
    if p_items: kw_lines.append("• 제품형태: " + ", ".join(p_items))
    if e_items: kw_lines.append("• 효능: " + ", ".join(e_items))
    if m_items: kw_lines.append("• 마케팅: " + ", ".join(m_items))
    if infl_names: kw_lines.append("• 인플루언서: " + ", ".join(infl_names))

    # 기본 통계·인사이트
    uniq_cnt = cur_base['key'].nunique()
    keep_med = int(pts['days'].median()) if not pts.empty else 0

    # 제품형태 상위 2개 집중도
    def share_two_top(base, rules):
        cnt = Counter()
        for nm in base['product'].dropna().astype(str):
            for label, pat in rules.items():
                if re.search(pat, nm, re.IGNORECASE): cnt[label]+=1
        total = sum(cnt.values())
        if total==0: return None, None, 0.0
        top2 = cnt.most_common(2)
        share = round((top2[0][1] + (top2[1][1] if len(top2)>1 else 0))*100/total, 1)
        labels = [x[0] for x in top2]
        return labels, total, share
    top2_labels, _, top2_share = share_two_top(cur_base, KW_PRODUCT)

    g_up = b.sort_values('delta', ascending=False).head(1)
    g_dn = b.sort_values('delta', ascending=True).head(1)
    up_txt = f"{g_up.iloc[0]['brand']}(+{round(g_up.iloc[0]['delta'],1)}/일)" if not g_up.empty and g_up.iloc[0]['delta']>0 else None
    dn_txt = f"{g_dn.iloc[0]['brand']}(-{abs(round(g_dn.iloc[0]['delta'],1))}/일)" if not g_dn.empty and g_dn.iloc[0]['delta']<0 else None

    price_bucket = None
    if med_price is not None:
        v = med_price
        if v < 10000: price_bucket = "1만원 미만"
        elif v < 20000: price_bucket = "1만대"
        elif v < 30000: price_bucket = "2만대"
        elif v < 40000: price_bucket = "3만대"
        else: price_bucket = "4만대+"

    promo_effect = None
    if (disc_promo is not None) and (disc_non is not None):
        diff = round(disc_promo - disc_non, 2)
        if diff >= 2.0:
            promo_effect = f"프로모션 평균 할인율이 일반 대비 +{diff}%p 높음"

    insights = []
    insights.append(f"7일간 Top{topn} 유니크 제품 수 {uniq_cnt}개 · 유지일수 중앙값 {keep_med}일")
    if top2_labels: insights.append(f"제품형태는 {', '.join(top2_labels)} 중심(상위2 합 {top2_share}%)")
    if up_txt or dn_txt:
        bits = []
        if up_txt: bits.append("상승 " + up_txt)
        if dn_txt: bits.append("하락 " + dn_txt)
        insights.append(", ".join(bits))
    if price_bucket: insights.append(f"주요 가격대 {price_bucket}")
    if promo_effect: insights.append(promo_effect)

    res.update({
        'range': f"{start.date()}~{end.date()}",
        'top10_lines': top_lines,
        'brand_lines': brand_lines,
        'inout': inout_text,
        'heroes': to_links(heroes),
        'flash': to_links(flash),
        'discount_all': disc_all,
        'discount_promo': disc_promo,
        'discount_nonpromo': disc_non,
        'discount_delta_same': disc_delta_same,
        'median_price': med_price,
        'cat_top5': cat_pairs,
        'kw_lines': kw_lines,
        'insights': insights,
        'stats': {'unique_items': uniq_cnt, 'keep_days_median': keep_med, 'topn': topn}
    })
    return res

# ---------- 슬랙 포맷 ----------
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
    lines.append("🏆 Top10")
    lines.extend(s['top10_lines'] or ["데이터 없음"])
    lines.append("")
    lines.append("🍞 브랜드 개수(일평균)")
    lines.extend(s['brand_lines'] or ["데이터 없음"])
    lines.append("")
    lines.append(f"🔁 인앤아웃(교체): {s['inout']}")
    if s['heroes']: lines.append("🆕 신규 히어로: " + ", ".join(s['heroes']))
    if s['flash']:  lines.append("✨ 반짝 아이템: " + ", ".join(s['flash']))
    if s['cat_top5']: lines.append("📈 카테고리 상위: " + " · ".join(s['cat_top5']))
    if s['kw_lines']:
        lines.append("🔎 주간 키워드 분석")
        lines.extend(s['kw_lines'])
    # 가격/할인
    tail = []
    if s.get('median_price') is not None: tail.append("중위가격 " + (fmt_money(s['median_price'], src) or ""))
    disc_bits = []
    if s.get('discount_all') is not None:    disc_bits.append(f"전체 {s['discount_all']:.2f}%")
    if s.get('discount_promo') is not None:  disc_bits.append(f"프로모션 {s['discount_promo']:.2f}%")
    if s.get('discount_nonpromo') is not None: disc_bits.append(f"일반 {s['discount_nonpromo']:.2f}%")
    if s.get('discount_delta_same') is not None: disc_bits.append(f"(동일상품 차이 +{s['discount_delta_same']:.2f}%p)")
    if disc_bits: tail.append("평균 할인율 " + " · ".join(disc_bits))
    if tail: lines.append("💵 " + " / ".join(tail))
    # 인사이트
    if s.get('insights'):
        lines.append("")
        lines.append("🧠 최종 인사이트")
        for ln in s['insights']:
            lines.append(f"- {ln}")
    return "\n".join(lines)

# ---------- 엔트리(개별 전송 기본) ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", nargs="+", choices=ALL_SRCS + ["all"],
                        default=os.getenv("ONLY_SRC","all").split(","))
    parser.add_argument("--split", action="store_true", default=True,
                        help="소스별 개별 요약/슬랙 파일 생성(기본 ON)")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR","./data/daily"))
    parser.add_argument("--min-days", type=int, default=int(os.getenv("MIN_DAYS","3")))
    args = parser.parse_args()

    targets = []
    for s in (args.src if isinstance(args.src, list) else [args.src]):
        targets.extend(ALL_SRCS if s == "all" else [s])
    targets = [t for t in targets if t in ALL_SRCS]

    ud = load_unified(args.data_dir)
    combined = {}
    combined_txt = []

    for src in targets:
        s = summarize_week(ud, src, min_days=args.min_days)
        combined[src] = s
        text = format_slack_block(src, s)
        # 개별 파일 저장
        with open(f"weekly_summary_{src}.json","w",encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)
        with open(f"slack_{src}.txt","w",encoding="utf-8") as f:
            f.write(text)
        if not args.split: combined_txt.append(text)

    # 파이프라인용 전체 JSON
    print(json.dumps(combined, ensure_ascii=False, indent=2))

    if not args.split:
        with open("weekly_slack_message.txt","w",encoding="utf-8") as f:
            f.write("\n\n— — —\n\n".join(combined_txt))

if __name__ == "__main__":
    main()
