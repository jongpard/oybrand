# -*- coding: utf-8 -*-
"""
주간 리포트 HTML 생성기
- weekly_summary_{src}.json(각 5개)을 읽어 1개의 HTML로 합침
- 파일명: weekly_YYYY_MM_DD_YYYY_MM_DD.html
- stdout(표준출력)으로 생성된 파일 경로 1줄 출력
"""

import json
import os
import re
import html
from datetime import date, timedelta

SRC_ORDER = ["oy_kor", "oy_global", "amazon_us", "qoo10_jp", "daiso_kr"]
SRC_TITLES = {
    "oy_kor":    "올리브영 국내 Top100",
    "oy_global": "올리브영 글로벌 Top100",
    "amazon_us": "아마존 US Top100",
    "qoo10_jp":  "큐텐 재팬 뷰티 Top200",
    "daiso_kr":  "다이소몰 뷰티/위생 Top200",
}

def last_complete_week(today=None):
    today = today or date.today()
    wd = today.weekday()
    last_sun = today - timedelta(days=wd+1)
    start = last_sun - timedelta(days=6)
    return start, last_sun

def load_json_if_exists(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def deduce_range(summaries):
    for s in summaries:
        if not s:
            continue
        rng = s.get("range")
        if isinstance(rng, str) and re.match(r"^\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2}$", rng):
            return rng
    st, ed = last_complete_week()
    return f"{st:%Y-%m-%d}-{ed:%Y-%m-%d}"

def split_range(rng):
    m = re.match(r"^(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})$", rng)
    if not m:
        parts = rng.split("-")
        if len(parts) >= 6:
            left = "-".join(parts[:3])
            right = "-".join(parts[3:6])
            return left, right
        return rng, rng
    return m.group(1), m.group(2)

def h(tag, content, cls=None):
    cl = f' class="{cls}"' if cls else ""
    return f"<{tag}{cl}>{content}</{tag}>"

def li(txt): return f"<li>{txt}</li>"

def list_or_empty(items):
    if not items:
        return "<p>데이터 없음</p>"
    return "<ul>" + "".join(li(html.escape(x)) for x in items) + "</ul>"

def list_links(items):
    if not items:
        return "<p>데이터 없음</p>"
    buf = []
    for it in items:
        name = html.escape(it.get("name",""))
        url  = it.get("url","")
        a = f'<a href="{html.escape(url)}" target="_blank" rel="noopener">{name}</a>' if url else name
        meta = f'(유지 {it.get("days",0)}일 · 평균 {it.get("avg",0):.1f}위) ({html.escape(it.get("arrow",""))})'
        buf.append(li(f"{a} {meta}"))
    return "<ol>" + "".join(buf) + "</ol>"

def kv_list(d, show_ratio=None, denom=1):
    if not d:
        return "<p>데이터 없음</p>"
    rows = []
    for k, v in d.items():
        if show_ratio:
            ratio = f" ({round(v * 100.0 / max(1,denom), 1)}%)"
        else:
            ratio = ""
        rows.append(li(f"{html.escape(k)}: {v}개{ratio}"))
    return "<ul>" + "".join(rows) + "</ul>"

def section_for(src, s):
    title = SRC_TITLES[src]
    if not s:
        return h("section", h("h2", title) + "<p>데이터 없음</p>")
    blk = []
    blk.append(h("h2", f"{title} ({html.escape(s.get('range',''))})"))
    blk.append(h("h3", "Top10"))
    blk.append(list_links(s.get("top10_items", [])))
    blk.append(h("h3", "브랜드 개수(일평균)"))
    blk.append(list_or_empty(s.get("brand_lines", [])))
    blk.append(h("h3", "인앤아웃(교체)"))
    blk.append(h("p", f"일평균 {s.get('inout_avg',0)}개"))
    blk.append(h("h3", "신규 히어로(≥3일 유지)"))
    heroes = s.get("heroes", [])
    blk.append(h("p", "없음") if not heroes else h("p", " · ".join(html.escape(x) for x in heroes)))
    blk.append(h("h3", "반짝 아이템(≤2일)"))
    flashes = s.get("flashes", [])
    blk.append(h("p", "없음") if not flashes else h("p", " · ".join(html.escape(x) for x in flashes)))
    blk.append(h("h3", "통계"))
    blk.append("<ul>"
               + li(f"Top{s.get('topn', 100)} 등극 SKU : {s.get('unique_cnt',0)}개")
               + li(f"Top {s.get('topn', 100)} 유지 평균 : {s.get('keep_days_mean',0):.1f}일")
               + "</ul>")
    kw = s.get("kw", {})
    blk.append(h("h3", "주간 키워드 분석"))
    if not kw:
        blk.append("<p>데이터 없음</p>")
    else:
        uniq = kw.get("unique", 0)
        blk.append(h("p", f"유니크 SKU: {uniq}개"))
        blk.append(h("h4", "마케팅 키워드"))
        blk.append(kv_list(kw.get("marketing", {}), show_ratio=True, denom=uniq))
        if kw.get("influencers"):
            blk.append(h("h4", "인플루언서"))
            blk.append(kv_list(kw.get("influencers", {}), show_ratio=False))
        blk.append(h("h4", "성분 키워드"))
        blk.append(kv_list(kw.get("ingredients", {}), show_ratio=False))
    return h("section", "".join(blk))

def build_html(summaries, rng):
    start_str, end_str = split_range(rng)
    css = """
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Apple SD Gothic Neo,Malgun Gothic,Helvetica,Arial,sans-serif;line-height:1.6;color:#111}
    main{max-width:980px;margin:40px auto;padding:0 16px}
    h1{font-size:28px;margin:0 0 16px}
    h2{font-size:22px;margin:32px 0 8px;border-bottom:1px solid #eee;padding-bottom:6px}
    h3{font-size:18px;margin:18px 0 6px}
    h4{font-size:16px;margin:12px 0 4px;color:#444}
    section{margin-bottom:40px}
    ol{padding-left:24px}
    li{margin:4px 0}
    a{color:#0b66ff;text-decoration:none}
    a:hover{text-decoration:underline}
    .meta{color:#555}
    """
    head = f"""
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>주간 리포트 ({html.escape(rng)})</title>
    <style>{css}</style>
    """
    body_top = h("h1", f"주간 리포트 <span class='meta'>({html.escape(rng)})</span>")
    sections = [section_for(src, summaries.get(src)) for src in SRC_ORDER]
    return "<!doctype html><html><head>" + head + "</head><body><main>" + body_top + "".join(sections) + "</main></body></html>"

def main():
    summaries = {}
    loaded_list = []
    for src in SRC_ORDER:
        js = load_json_if_exists(f"weekly_summary_{src}.json")
        summaries[src] = js
        loaded_list.append(js)
    rng = deduce_range(loaded_list)
    html_str = build_html(summaries, rng)
    s, e = split_range(rng)
    out_name = f"weekly_{s.replace('-','_')}_{e.replace('-','_')}.html"
    with open(out_name, "w", encoding="utf-8") as f:
        f.write(html_str)
    print(out_name)

if __name__ == "__main__":
    main()
