# -*- coding: utf-8 -*-
"""
주간 HTML 리포트 생성 (클릭 가능한 링크 포함)
- weekly_summary_{src}.json 5개를 읽어 한 장의 HTML로 합침
- 섹션/소제목 볼드, Top10 항목은 <a href> 링크
출력: ./weekly_YYYY_MM_DD_YYYY_MM_DD.html
"""

import glob
import json
import os
from datetime import datetime

SRC_ORDER = ["oy_kor", "oy_global", "amazon_us", "qoo10_jp", "daiso_kr"]

def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _escape(t: str) -> str:
    return (t or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def build_section(src: str, data: dict) -> str:
    h = []
    h.append(f'<h2>📈 {data["title"]} <small>({_escape(data["range"])})</small></h2>')

    # Top10
    h.append("<h3><b>Top10</b></h3>")
    if data.get("top10_items"):
        h.append("<ol>")
        for it in data["top10_items"]:
            name = _escape(it["name"])
            if it.get("url"):
                name = f'<a href="{it["url"]}" target="_blank" rel="noopener">{name}</a>'
            meta = f'(유지 {it["days"]}일 · 평균 {it["avg"]:.1f}위) ({_escape(it["arrow"])})'
            h.append(f"<li>{name} {meta}</li>")
        h.append("</ol>")
    else:
        h.append("<p>데이터 없음</p>")

    # 브랜드
    h.append("<h3><b>브랜드 개수(일평균)</b></h3>")
    if data.get("brand_lines"):
        h.append("<ul>")
        for ln in data["brand_lines"]:
            h.append(f"<li>{_escape(ln)}</li>")
        h.append("</ul>")
    else:
        h.append("<p>데이터 없음</p>")

    # 인앤아웃
    h.append("<h3><b>인앤아웃(교체)</b></h3>")
    h.append(f'<p>일평균 {_escape(str(data.get("inout_avg", 0)))}개</p>')

    # 신규/반짝
    h.append("<h3><b>신규 히어로(≥3일 유지)</b></h3>")
    if data.get("heroes"):
        h.append("<p>" + " · ".join(_escape(x) for x in data["heroes"]) + "</p>")
    else:
        h.append("<p>없음</p>")
    h.append("<h3><b>반짝 아이템(≤2일)</b></h3>")
    if data.get("flashes"):
        h.append("<p>" + " · ".join(_escape(x) for x in data["flashes"]) + "</p>")
    else:
        h.append("<p>없음</p>")

    # 통계
    h.append("<h3><b>통계</b></h3>")
    h.append("<ul>")
    h.append(f'<li>Top{data["topn"]} 등극 SKU : {data.get("unique_cnt",0)}개</li>')
    h.append(f'<li>Top {data["topn"]} 유지 평균 : {data.get("keep_days_mean",0.0)}일</li>')
    h.append("</ul>")

    # 키워드
    kw = data.get("kw", {})
    h.append("<h3><b>주간 키워드 분석</b></h3>")
    if kw and kw.get("unique", 0) > 0:
        h.append(f'<p>유니크 SKU: {kw["unique"]}개</p>')
        # 마케팅
        if kw.get("marketing"):
            h.append("<p><b>마케팅 키워드</b></p><ul>")
            for k, cnt in kw["marketing"].items():
                ratio = round(cnt * 100.0 / max(1, kw["unique"]), 1)
                h.append(f"<li>{_escape(k)}: {cnt}개 ({ratio}%)</li>")
            h.append("</ul>")
        # 인플(oy_kor만 의미)
        if kw.get("influencers"):
            h.append("<p><b>인플루언서</b></p><ul>")
            for k, cnt in kw["influencers"].items():
                h.append(f"<li>{_escape(k)}: {cnt}개</li>")
            h.append("</ul>")
        # 성분
        if kw.get("ingredients"):
            h.append("<p><b>성분 키워드</b></p><ul>")
            for k, cnt in kw["ingredients"].items():
                h.append(f"<li>{_escape(k)}: {cnt}개</li>")
            h.append("</ul>")
    else:
        h.append("<p>데이터 없음</p>")

    return "\n".join(h)

def main():
    # 요약 파일 모으기
    files = {src: f"weekly_summary_{src}.json" for src in SRC_ORDER}
    data = {}
    any_range = None
    for src, fp in files.items():
        if os.path.exists(fp):
            data[src] = _load(fp)
            any_range = data[src]["range"]
        else:
            data[src] = {"title": fp.replace("weekly_summary_", "").replace(".json",""),
                         "range": any_range or "", "topn": 100,
                         "top10_items": [], "brand_lines": [], "inout_avg": 0,
                         "heroes": [], "flashes": [], "kw": {"unique":0},
                         "unique_cnt": 0, "keep_days_mean": 0.0}

    # 문서
    head = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>주간 리포트</title>
<style>
body{font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans KR', Arial, sans-serif; padding:24px; line-height:1.56;}
h1{margin-top:0}
h2{margin-top:32px; border-top:1px solid #eee; padding-top:16px}
h3{margin:16px 0 8px}
small{color:#888}
ul{margin:8px 0 16px 20px}
ol{margin:8px 0 16px 20px}
code{background:#f6f8fa; padding:2px 4px; border-radius:4px}
</style>
</head>
<body>
"""
    body = [f"<h1><b>주간 리포트</b> <small>({any_range or ''})</small></h1>"]
    for src in SRC_ORDER:
        body.append(build_section(src, data[src]))
    body.append("</body></html>")
    html = head + "\n".join(body)

    # 파일명: weekly_YYYY_MM_DD_YYYY_MM_DD.html
    if any_range:
        s, e = any_range.split("-")
        out = f"weekly_{s.replace('-','_')}_{e.replace('-','_')}.html"
    else:
        out = f"weekly_{datetime.now():%Y_%m_%d}.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(out)

if __name__ == "__main__":
    main()
