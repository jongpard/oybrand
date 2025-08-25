# -*- coding: utf-8 -*-
import json
from pathlib import Path
from format_utils import load_cfg, make_link, inline_list

CFG = load_cfg()
ARW = CFG["arrows"]
SRC_ORDER = ["oy_kor","oy_global","amazon_us","qoo10_jp","daiso_kr"]

def line_top10_html(it):
    raw = it.get("raw") or it.get("raw_name") or it.get("name") or ""
    url = it.get("url")
    days = it.get("keep_days", 0)
    avg_rank = it.get("avg_rank")
    delta = it.get("delta")
    arrow = ARW["flat"]
    if isinstance(delta, (int,float)):
        arrow = ARW["up"] if delta < 0 else (ARW["down"] if delta > 0 else ARW["flat"])
    raw_fmt = make_link(raw, url, target="html")
    tail = ""
    if CFG["trend"]["parentheses"]:
        tail = f" (유지 {days}일 · 평균 {avg_rank:.1f}위)" if avg_rank is not None else f" (유지 {days}일)"
    return f"{raw_fmt} {arrow}{tail}".strip()

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def section_html(title, lines):
    inner = "".join(f"<li>{l}</li>" for l in lines)
    return f"<h3>{html_escape(title)}</h3><ol>{inner}</ol>"

def block_stats_html(s):
    uniq = s.get("unique_cnt", 0)
    keep = s.get("keep_days_mean", 0.0)
    return (
        f"<h4>📦 통계</h4>"
        f"<ul><li>Top 100 등극 SKU: {uniq}개</li>"
        f"<li>Top 100 유지 평균: {keep:.1f}일</li></ul>"
    )

def block_keywords_html(s):
    kw = s.get("kw", {}) or {}
    rows = []
    for key, title in [
        ("product_type","제품형태"),("benefits","효능"),
        ("marketing","마케팅"),("ingredients","성분"),("influencers","인플루언서")
    ]:
        vals = kw.get(key, [])
        if vals:
            rows.append(f"<li><b>{title}:</b> {inline_list(vals)}</li>")
    if not rows:
        return ""
    return "<h4>🔎 주간 키워드 분석</h4><ul>" + "".join(rows) + "</ul>"

def main():
    outdir = Path("dist"); outdir.mkdir(parents=True, exist_ok=True)
    summary = json.loads(Path("weekly_summary.json").read_text(encoding="utf-8"))
    html_parts = ['<meta charset="utf-8"><style>body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.5;padding:20px} h2{margin:24px 0 8px} h3{margin:16px 0 8px} ol,ul{margin:8px 0 16px 20px}</style>']
    html_parts.append("<h2>주간 리포트</h2>")

    for src in SRC_ORDER:
        if src not in summary: 
            continue
        s = summary[src]
        title = s.get("title", src)
        rng = s.get("range", "")
        html_parts.append(f"<h2>📈 {title} ({html_escape(rng)})</h2>")

        items = s.get("top10_items", [])[:10]
        if items:
            lines = [line_top10_html(it) for it in items]
            html_parts.append(section_html("🏆 Top10", lines))
        else:
            html_parts.append("<p>데이터 없음</p>")

        html_parts.append(block_stats_html(s))
        kw_html = block_keywords_html(s)
        if kw_html:
            html_parts.append(kw_html)

        heroes = s.get("heroes") or []
        if heroes:
            lines = [line_top10_html(it) for it in heroes]
            html_parts.append(section_html("🔥 히어로(3일 이상 랭크 유지)", lines))
        flashes = s.get("flashes") or []
        if flashes:
            lines = [line_top10_html(it) for it in flashes]
            html_parts.append(section_html("✨ 반짝 아이템(2일 이내 랭크 아웃)", lines))

    out = Path("dist/weekly_report.html")
    out.write_text("".join(html_parts), encoding="utf-8")
    print(f"HTML_FILE={out}")
    # GitHub Actions에서 사용 가능하도록 이름만 echo
    Path("dist/BUILD_HTML_DONE").write_text(str(out), encoding="utf-8")

if __name__ == "__main__":
    main()
