# -*- coding: utf-8 -*-
import os, re

# ── 1) 올영픽 vs PICK(인플루언서) 분리 ─────────────────────────────
RE_OY_PICK  = re.compile(r'(올영픽|올리브영\s*픽)\b', re.I)
RE_INFL_PK  = re.compile(r'([가-힣A-Za-z0-9.&/_-]+)\s*(픽|Pick)\b', re.I)
EXCLUDE_INFL = {'올영', '올리브영', '월올영', '원픽'}  # 인플에서 제외

# 마케팅 키워드: **각 항목을 별도 집계(묶지 않음)**
PAT_MARKETING = {
    '올영픽'   : r'(올영픽|올리브영\s*픽)',      # 프로모션(인플 아님)
    '특가'     : r'(특가|핫딜|세일|할인)',
    '세트'     : r'(세트|구성|트리오|듀오|패키지|킷\b|키트\b)',
    '기획'     : r'(기획|기획전)',
    '1+1/증정' : r'(1\+1|1\+2|덤|증정|사은품)',
    '한정/NEW' : r'(한정|리미티드|NEW|뉴\b)',
    '쿠폰/딜'  : r'(쿠폰|딜\b|딜가|프로모션|프로모\b)',
}
PAT_MARKETING = {k: re.compile(v, re.I) for k, v in PAT_MARKETING.items()}

def parse_marketing_and_infl(raw_name: str):
    name = raw_name or ''
    # 마케팅(각 항목 개별 플래그)
    mk = {k: bool(p.search(name)) for k, p in PAT_MARKETING.items()}

    # 인플루언서: “… Pick/픽” 앞 단어를 추출하되, 올영/월올영 등은 제외
    infl = None
    m = RE_INFL_PK.search(name)
    if m:
        cand = re.sub(r'[\[\](),.|·]', '', m.group(1)).strip()
        if cand and cand not in EXCLUDE_INFL and not RE_OY_PICK.search(name):
            infl = cand
    return mk, infl

# ── 2) 성분 키워드: 외부 파일로 확장 가능 ──────────────────────────
# 기본(백업) 리스트
DEFAULT_INGRS = [
    '히알루론산','세라마이드','나이아신아마이드','레티놀','펩타이드','콜라겐',
    '비타민C','BHA','AHA','PHA','판테놀','센텔라','마데카소사이드',
]

def load_ingredients():
    """
    configs/ingredients.txt 에서 한 줄 한 키워드(주석 # 허용).
    파일이 없으면 DEFAULT_INGRS 사용.
    """
    path = os.path.join('configs', 'ingredients.txt')
    if not os.path.exists(path):
        return DEFAULT_INGRS
    words = []
    with open(path, 'r', encoding='utf-8') as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith('#'):
                continue
            words.append(ln)
    return words or DEFAULT_INGRS

def extract_ingredients(raw_name: str, ingr_list=None):
    ingr_list = ingr_list or load_ingredients()
    name = raw_name or ''
    found = []
    for w in ingr_list:
        if re.search(re.escape(w), name, re.I):
            found.append(w)
    return found
