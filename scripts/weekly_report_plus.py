# scripts/weekly_report_plus.py
# -*- coding: utf-8 -*-
import os, re, glob, json, math, argparse
import pandas as pd
from collections import Counter
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

# --------- 설정 ---------
SRC_INFO = {
    'oy_kor':    {'topn':100, 'hints':['올리브영_랭킹','oliveyoung_kor','oy_kor'], 'currency':'KRW'},
    'oy_global': {'topn':100, 'hints':['올리브영글로벌','oliveyoung_global','oy_global'], 'currency':'KRW'},
    'amazon_us': {'topn':100, 'hints':['아마존US','amazon_us','amazonUS'],       'currency':'USD'},
    'qoo10_jp':  {'topn':200, 'hints':['큐텐재팬','qoo10','Qoo10'],               'currency':'JPY'},
    'daiso_kr':  {'topn':200, 'hints':['다이소몰','daiso'],                       'currency':'KRW'},
}
ALL_SRCS = list(SRC_INFO.keys())

# 반짝 기준(최대 유지일수)
FLASH_MAX_DAYS = int(os.getenv("FLASH_MAX_DAYS", "2"))

# 컬럼 후보
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

# 키워드 룰
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
}
# 성분 키워드
KW_INGREDIENT = {
    '히알루론산': r'(히알루론|hyaluronic|HA\b)',
    '니아신아마이드': r'(니아신|niacinamide|vitamin\s*B3)',
    '비타민C': r'(비타민\s*C|ascorbic\s*acid|AA\b|VC\b)',
    '센텔라/시카': r'(센텔라|병풀|cica|madecassoside|asiaticoside)',
    '세라마이드': r'(세라마이드|ceramide)',
    '콜라겐': r'(콜라겐|collagen)',
    '펩타이드': r'(펩타이드|peptide)',
    '레티놀/레티날': r'(레티놀|레티날|retinol|retinal)',
    '판테놀': r'(판테놀|panthenol)',
    'PDRN': r'(\bPDRN\b)',
    '살리실산/BHA': r'(살리실|salicylic|BHA\b)',
    'AHA/PHA': r'(\bAHA\b|\bPHA\b)',
    '징크옥사이드': r'(징크\s*옥사이드|zinc\s*oxide)',
}

# 카테고리 매핑(간단 룰)
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

# --------- 유틸 ---------
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
            if h.lower() in base or h.lower() in parent:
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

def fmt_money(v, src):
    if v is None or (isinstance(v,float) and (pd.isna(v) or math.isnan(v))): return None
    cur = SRC_INFO[src]['currency']
    if cur == 'USD':  return f"${v:,.0f}"
    if cur == 'JPY':  return f"¥{v:,.0f}"
    return f"₩{v:,.0f}"

# 프로모션/인플루언서
PROMO_RE = re.compile(
    r"(올영픽|특가|기획|증정|세일|sale|event|행사|한정|리미티드|1\+1|2\+1|더블\s*기획|증량|쿠폰|coupon|deal|딜|gift|bundle|promotion|NEW\b|신상)",
    re.IGNORECASE
)
INF_BLACK = {'올영','월올영','원더','MD','에디터','브랜드','editor','brand','oliveyoung','스토어','공식','픽'}

def is_promo(name:str)->bool:
    n = (name or "")
    if re.search(r'픽\b', n):  # 한글 '픽'은 모두 프로모션 취급
        return True
    return bool(PROMO_RE.search(n))

def extract_influencers_dynamic(name:str):
    """영어 pick/PICK 앞 단어 or ×/with/콜라보 패턴만 인플로 인식"""
    if not name: return set()
    t = str(name)
    out = set()
    # 영어 pick 앞 단어
    for m in re.finditer(r'([가-힣A-Za-z]{2,20})\s*(?:pick|PICK)\b', t):
        nm = m.group(1).strip()
        if nm and nm not in INF_BLACK and nm.lower() not in {x.lower() for x in INF_BLACK}:
            out.add(nm)
    # × / with / 콜라보
    for m in re.finditer(r'([가-힣A-Za-z]{2,20})\s*(?:×|x|X|with|콜라보|collab(?:oration)?)\s*([가-힣A-Za-z]{2,20})', t, re.IGNORECASE):
        for nm in (m.group(1), m.group(2)):
            nm = nm.strip()
            if nm and nm not in INF_BLACK and nm.lower() not in {x.lower() for x in INF_BLACK}:
                if not re.search(r'\d|ml|g|pack|set|기획|특가|세트', nm, re.IGNORECASE):
                    out.add(nm)
    return out

def daily_topn_base(df, topn):
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
    if diff_rank is None or (isinstance(diff_rank,float) and math.isnan(diff_rank)): return "—"
    d = int(round(diff_rank))
    if d > 0:  return f"↑{d}"
    if d < 0:  return f"↓{abs(d)}"
    return "—"

# --------- 로딩 ---------
def load_unified(data_dir:str)->pd.DataFrame:
    paths = glob.glob(os.path.join(data_dir,'**','*.csv'), recursive=True)
    rows=[]
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

        prod_col = pick_loose_product(df)
        brand_col = pick(df, COLS['brand'])
        url_col   = pick(df, COLS['url'])
        price_col = pick(df, COLS['price'])
        orig_col  = pick(df, COLS['orig_price'])
        disc_col  = pick(df, COLS['discount'])

        for i,r in df.iterrows():
            nm = str(r.get(prod_col)) if prod_col else None
            br = str(r.get(brand_col)) if brand_col else None
            # 아마존 'Amazon' → 'Amazon Basics'로 통일
            if src == 'amazon_us' and br and br.strip().lower()=='amazon':
                br='Amazon Basics'
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
    ud = pd.DataFrame(rows, columns=['source','date','rank','product','brand','url','price','orig_price','discount_rate','key','promo','infl'])
    ud = ud.dropna(subset=['source','date','rank','key'])
    return ud

# --------- 집계 ---------
def summarize_week(ud:pd.DataFrame, src:str, min_days:int=3):
    res = {'range':'데이터 없음','top10_lines':['데이터 없음'],'brand_lines':['데이터 없음'],
           'inout':'','heroes':[],'flash':[],'discount_all':None,'discount_promo':None,
           'discount_nonpromo':None,'discount_delta_same':None,'median_price':None,
           'cat_top5':[],'kw_lines':[],'insights':[],'stats':{}}
    rng = week_range_for_source(ud, src)
    if not rng: return res
    start,end = rng; topn = SRC_INFO[src]['topn']

    cur = ud[(ud['source'].eq(src)) & (ud['date']>=start) & (ud['date']<=end) & (ud['rank']<=topn)].copy()
    if cur.empty:
        res['range']=f"{start.date()}~{end.date()}"; return res
    prev = ud[(ud['source'].eq(src)) & (ud['date']>=start-pd.Timedelta(days=7)) & (ud['date']<=end-pd.Timedelta(days=7)) & (ud['rank']<=topn)].copy()
    hist = ud[(ud['source'].eq(src)) & (ud['date']>=start-pd.Timedelta(days=28)) & (ud['date']<=start-pd.Timedelta(days=1)) & (ud['rank']<=topn)].copy()

    cur_base  = daily_topn_base(cur, topn)
    prev_base = daily_topn_base(prev, topn)

    # 주간 포인트 테이블 두 벌(반짝 버그 방지)
    tmp = cur_base.copy()
    tmp['__pts'] = topn + 1 - tmp['rank']
    pts_all = (tmp.groupby('key', as_index=False)
                 .agg(points=('__pts','sum'),
                      days=('rank','count'),
                      best=('rank','min'),
                      mean_rank=('rank','mean')))
    pts_stable = pts_all[pts_all['days'] >= min_days]

    # 최신 정보
    latest = (cur_base.sort_values('date')
                .groupby('key', as_index=False)
                .agg(product=('product','last'),
                     brand=('brand','last'),
                     url=('url','last')))

    # 이전주 평균순위/일수 맵
    prev_tbl=None; prev_mean_map={}; prev_days_map={}
    if not prev_base.empty:
        ptmp=prev_base.copy(); ptmp['__pts']=topn+1-ptmp['rank']
        prev_tbl=(ptmp.groupby('key', as_index=False)
                    .agg(points=('__pts','sum'), days=('rank','count'), best=('rank','min'), mean_rank=('rank','mean')))
        prev_mean_map=dict(zip(prev_tbl['key'], prev_tbl['mean_rank']))
        prev_days_map=dict(zip(prev_tbl['key'], prev_tbl['days']))

    # Top10
    top = (pts_stable.merge(latest,on='key',how='left')
             .sort_values(['points','days','best'], ascending=[False,False,True]).head(10))

    top_lines=[]
    for i,r in enumerate(top.itertuples(),1):
        key=getattr(r,'key'); nm=getattr(r,'product') or getattr(r,'brand') or key
        u=getattr(r,'url') or ''
        label=f"<{u}|{nm}>" if u else nm
        cur_mean=getattr(r,'mean_rank'); prev_mean=prev_mean_map.get(key); prev_days=prev_days_map.get(key,0)
        delta_txt="NEW" if (prev_mean is None or prev_days<min_days) else _arrow_rank(prev_mean-cur_mean)
        mean_txt=f"{round(float(cur_mean),1)}위" if cur_mean is not None else "-"
        top_lines.append(f"{i}. {label} (유지 {int(getattr(r,'days'))}일 · 평균 {mean_txt}) ({delta_txt})")

    # 브랜드(일평균 개수, 증감)
    def brand_daily_avg(base):
        return (base.groupby(['day','brand']).size().groupby('brand').mean().reset_index(name='per_day'))
    b_now = brand_daily_avg(cur_base).rename(columns={'per_day':'now'})
    b_prev = brand_daily_avg(prev_base).rename(columns={'per_day':'prev'}) if not prev_base.empty else pd.DataFrame(columns=['brand','prev'])
    b = (b_now.merge(b_prev,on='brand',how='left').fillna(0.0)
            .assign(delta=lambda x:x['now']-x['prev'])
            .sort_values(['now','delta'], ascending=[False,False]).head(12))
    brand_lines=[]
    for r in b.itertuples():
        sign = f"↑{round(r.delta,1)}" if r.delta>0 else (f"↓{abs(round(r.delta,1))}" if r.delta<0 else "—")
        brand_lines.append(f"{r.brand} {round(r.now,1)}개/일 ({sign})")

    # IN(교체 수) – OUT은 표시하지 않음
    days = sorted(cur_base['day'].unique())
    prev_days_set = set(prev_base['day'].unique())
    total_in = 0
    valid = 0
    for d in days:
        pd_ = pd.to_datetime(d) - pd.Timedelta(days=1)
        if pd_.date() not in prev_days_set: continue
        cur_set  = set(cur_base.loc[cur_base['day'].eq(d),'key'])
        prev_set = set(prev_base.loc[prev_base['day'].eq(pd_.date()),'key'])
        total_in += len(cur_set - prev_set)
        valid += 1
    in_avg = round(total_in/valid, 1) if valid else 0.0
    inout_line = "비교 기준 없음" if valid==0 else f"일평균 {in_avg:.1f}개"

    # 히어로 / 반짝
    hist_keys=set(hist['key'].unique()) if not hist.empty else set()
    heroes=(pts_stable[~pts_stable['key'].isin(hist_keys)].merge(latest,on='key',how='left')
              .sort_values(['points','days','best'], ascending=[False,False,True]).head(5))
    flash=(pts_all[(pts_all['days']<=FLASH_MAX_DAYS)].merge(latest,on='key',how='left')
              .sort_values(['points','days','best'], ascending=[False,False,True]).head(5))

    def to_links(df):
        if df is None or df.empty: return []
        out=[]
        for r in df.itertuples():
            nm=getattr(r,'product') or getattr(r,'brand') or getattr(r,'key'); u=getattr(r,'url') or ''
            out.append(f"<{u}|{nm}>" if u else nm)
        return out

    # 가격/할인
    wk=(cur_base.groupby('key').agg(price_med=('price','median'), disc_avg=('discount_rate','mean')).reset_index())
    med_price=int(wk['price_med'].dropna().median()) if wk['price_med'].notna().any() else None

    promo_base=cur_base[cur_base['promo']==True]; non_base=cur_base[cur_base['promo']!=True]
    def _mean_disc(df): return round(float(df['discount_rate'].dropna().mean()),2) if df['discount_rate'].notna().any() else None
    disc_all=_mean_disc(cur_base); disc_promo=_mean_disc(promo_base); disc_non=_mean_disc(non_base)

    both=(cur_base.groupby(['key','promo'])['discount_rate'].mean().reset_index()
                .pivot(index='key', columns='promo', values='discount_rate').dropna(how='all'))
    disc_delta_same=None
    if not both.empty and True in both.columns and False in both.columns:
        diff=(both[True]-both[False]).dropna()
        if not diff.empty: disc_delta_same=round(float(diff.mean()),2)

    # 카테고리 상위(유니크 제품 대비 %)
    uniq = cur_base.sort_values('date').drop_duplicates('key')[['key','product']]
    def map_cat(name:str)->str:
        nm=(name or "").lower()
        for cat,pat in CATEGORY_RULES:
            if re.search(pat,nm,re.IGNORECASE): return cat
        return "기타"
    cats = uniq.copy(); cats['__cat']=cats['product'].map(map_cat)
    cat_cnt = cats['__cat'].value_counts()
    total_uniq = len(uniq)
    cat_pairs = [f"{c} {round(n*100/total_uniq,1)}%" for c,n in cat_cnt.head(5).items()]

    # 키워드(유니크 제품 기준 %)
    def share_unique(base, rules):
        keys = base.sort_values('date').drop_duplicates('key')[['key','product']]
        total = len(keys); cnt = Counter()
        for row in keys.itertuples():
            name=getattr(row,'product') or ''
            for label,pat in rules.items():
                if re.search(pat, name, re.IGNORECASE):
                    cnt[label]+=1
        if total==0: return []
        return [f"{k} {round(v*100/total,1)}%" for k,v in cnt.most_common(6)]

    kw_lines=[]
    p_items=share_unique(cur_base, KW_PRODUCT)
    e_items=share_unique(cur_base, KW_EFFICACY)
    m_items=share_unique(cur_base, KW_MARKETING)
    ing_items=share_unique(cur_base, KW_INGREDIENT)
    if p_items: kw_lines.append("• 제품형태: " + ", ".join(p_items))
    if e_items: kw_lines.append("• 효능: " + ", ".join(e_items))
    if m_items: kw_lines.append("• 마케팅: " + ", ".join(m_items))
    if ing_items: kw_lines.append("• 성분: " + ", ".join(ing_items))

    # 인플루언서(올리브영 국내만)
    if src=='oy_kor':
        icnt=Counter()
        for names in cur_base['infl'].dropna():
            for n in names: icnt[n]+=1
        infl_names=[n for n,_ in icnt.most_common(8)]
        if infl_names: kw_lines.append("• 인플루언서: " + ", ".join(infl_names))

    # 가격대 버킷(소스별)
    def price_bucket(src, med):
        if med is None: return None
        if src=='amazon_us':
            if med<10:  return "$10 미만"
            if med<20:  return "$10대"
            if med<30:  return "$20대"
            if med<40:  return "$30대"
            return "$40대+"
        if src=='qoo10_jp':
            if med<1000:  return "¥1천 미만"
            if med<2000:  return "¥1천대"
            if med<3000:  return "¥2천대"
            if med<4000:  return "¥3천대"
            return "¥4천대+"
        if src=='daiso_kr':
            if med<2000: return "2천 미만"
            if med<3000: return "2천대"
            if med<5000: return "3~4천대"
            return "5천+"
        # default KRW
        if med<10000: return "1만 미만"
        if med<20000: return "1만대"
        if med<30000: return "2만대"
        if med<40000: return "3만대"
        return "4만+"
    price_bucket_txt = price_bucket(src, med_price)

    # 인사이트 (중복 제거: TopN 등극 SKU는 제외)
    keep_mean = round(float(pts_all['days'].mean()), 1) if not pts_all.empty else 0.0
    keep_med  = int(pts_stable['days'].median()) if not pts_stable.empty else 0
    g_up=b.sort_values('delta',ascending=False).head(1)
    g_dn=b.sort_values('delta',ascending=True).head(1)
    up_txt = f"{g_up.iloc[0]['brand']}(+{round(g_up.iloc[0]['delta'],1)}/일)" if not g_up.empty and g_up.iloc[0]['delta']>0 else None
    dn_txt = f"{g_dn.iloc[0]['brand']}(-{abs(round(g_dn.iloc[0]['delta'],1))}/일)" if not g_dn.empty and g_dn.iloc[0]['delta']<0 else None
    promo_effect=None
    if (disc_promo is not None) and (disc_non is not None):
        diff=round(disc_promo-disc_non,2)
        if abs(diff)>=2.0: promo_effect=f"프로모션 평균 할인율이 일반 대비 {('+' if diff>0 else '')}{diff}%p"

    insights=[f"Top {topn} 유지 평균 {keep_mean}일"]  # ← 요청대로 표기
    if up_txt or dn_txt:
        bits=[]
        if up_txt: bits.append("상승 "+up_txt)
        if dn_txt: bits.append("하락 "+dn_txt)
        insights.append(", ".join(bits))
    if price_bucket_txt: insights.append(f"주요 가격대 {price_bucket_txt}")
    if promo_effect: insights.append(promo_effect)

    res.update({
        'range': f"{start.date()}~{end.date()}",
        'top10_lines': top_lines,
        'brand_lines': brand_lines,
        'inout': inout_line,           # 🔁 인앤아웃(교체): "일평균 n.n개"
        'heroes': to_links(heroes),
        'flash': to_links(flash),
        'discount_all': disc_all,
        'discount_promo': disc_promo,
        'discount_nonpromo': disc_non,
        'discount_delta_same': disc_delta_same,
        'median_price': med_price,
        'cat_top5': [f"{x}" for x in cat_pairs],
        'kw_lines': kw_lines,
        'insights': insights,
        'stats': {
            'unique_items': total_uniq,
            'keep_days_mean': keep_mean,
            'keep_days_median': keep_med,
            'topn': topn
        }
    })
    return res

# --------- 슬랙 포맷 ---------
def format_slack_block(src:str, s:dict)->str:
    title_map={'oy_kor':"올리브영 국내 Top100",'oy_global':"올리브영 글로벌 Top100",
               'amazon_us':"아마존 US Top100",'qoo10_jp':"큐텐 재팬 뷰티 Top200",'daiso_kr':"다이소몰 뷰티/위생 Top200"}
    L=[]
    L.append(f"📊 주간 리포트 · {title_map.get(src,src)} ({s['range']})")
    L.append("🏆 Top10"); L.extend(s.get('top10_lines') or ["데이터 없음"]); L.append("")
    L.append("🍞 브랜드 개수(일평균)"); L.extend(s.get('brand_lines') or ["데이터 없음"]); L.append("")
    # 인앤아웃(교체): 일평균만
    L.append(f"🔁 인앤아웃(교체): {s.get('inout','비교 기준 없음')}")
    # 히어로/반짝 – 기준 설명 추가
    L.append("🆕 신규 히어로(3일 이상 랭크 유지): " + (", ".join(s.get('heroes') or []) if s.get('heroes') else "없음"))
    L.append("✨ 반짝 아이템(2일 이내 랭크 아웃): " + (", ".join(s.get('flash')  or []) if s.get('flash')  else "없음"))
    if s.get('cat_top5'): L.append("📈 카테고리 상위: " + " · ".join(s['cat_top5']))
    if s.get('kw_lines'):
        L.append("🔎 주간 키워드 분석"); L.extend(s['kw_lines'])
    tail=[]
    if s.get('median_price') is not None: tail.append("중위가격 " + (fmt_money(s['median_price'], src) or ""))
    disc=[]
    if s.get('discount_all')       is not None: disc.append(f"전체 {s['discount_all']:.2f}%")
    if s.get('discount_promo')     is not None: disc.append(f"프로모션 {s['discount_promo']:.2f}%")
    if s.get('discount_nonpromo')  is not None: disc.append(f"일반 {s['discount_nonpromo']:.2f}%")
    if s.get('discount_delta_same') is not None: disc.append(f"(동일상품 차이 {('+' if s['discount_delta_same']>=0 else '')}{s['discount_delta_same']:.2f}%p)")
    if disc: tail.append("평균 할인율 " + " · ".join(disc))
    if tail: L.append("💵 " + " / ".join(tail))
    if s.get('insights'):
        L.append(""); L.append("🧠 최종 인사이트")
        for x in s['insights']: L.append(f"- {x}")
    return "\n".join(L)

# --------- 엔트리 ---------
def _parse_src_args(arg_src):
    """--src가 콤마/공백 혼용돼도 안전하게 파싱"""
    if isinstance(arg_src, list):
        raw = arg_src
    else:
        raw = [arg_src]
    tokens=[]
    for t in raw:
        tokens += [p for p in re.split(r'[,\s]+', str(t)) if p]
    targets=[]
    for s in tokens:
        if s == "all":
            targets.extend(ALL_SRCS)
        elif s in ALL_SRCS:
            targets.append(s)
    # 중복 제거
    return [t for i,t in enumerate(targets) if t not in targets[:i]]

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--src", nargs="+", default=os.getenv("ONLY_SRC","all"))
    parser.add_argument("--split", action="store_true", default=True, help="소스별 개별 파일 생성(기본 ON)")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR","./data/daily"))
    parser.add_argument("--min-days", type=int, default=int(os.getenv("MIN_DAYS","3")))
    args=parser.parse_args()

    targets=_parse_src_args(args.src)
    if not targets: targets = ALL_SRCS

    ud=load_unified(args.data_dir)
    combined={}; combined_txt=[]
    for src in targets:
        s=summarize_week(ud, src, min_days=args.min_days)
        combined[src]=s
        text=format_slack_block(src, s)
        with open(f"weekly_summary_{src}.json","w",encoding="utf-8") as f: json.dump(s,f,ensure_ascii=False,indent=2)
        with open(f"slack_{src}.txt","w",encoding="utf-8") as f: f.write(text)
        if not args.split: combined_txt.append(text)

    print(json.dumps(combined, ensure_ascii=False, indent=2))
    if not args.split:
        with open("weekly_slack_message.txt","w",encoding="utf-8") as f:
            f.write("\n\n— — —\n\n".join(combined_txt))

if __name__=="__main__":
    main()
