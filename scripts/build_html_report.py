# -*- coding: utf-8 -*-
import os, json, html
from datetime import datetime

SRC_INFO = [
    ("oy_kor",    "올리브영 국내 Top100"),
    ("oy_global", "올리브영 글로벌 Top100"),
    ("amazon_us", "아마존 US Top100"),
    ("qoo10_jp",  "큐텐 재팬 뷰티 Top200"),
    ("daiso_kr",  "다이소몰 뷰티/위생 Top200"),
]

def read_text(path: str) -> str:
    if not os.path.exists(path):
        return "데이터 없음"
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def find_range() -> str:
    """weekly_summary_{src}.json 중 존재하는 첫 파일의 range를 제목/파일명에 사용."""
    for key, _ in SRC_INFO:
        p = f"weekly_summary_{key}.json"
        if os.path.exists(p):
            try:
                r = json.load(open(p, "r", encoding="utf-8")).get("range")
                if r:
                    return r  # 예: "2025-08-18-2025-08-24"
            except Exception:
                pass
    # fallback: 오늘 날짜
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return f"{today}"

def build_html(range_str: str) -> str:
    parts = []
    parts.append(f"""<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"/>
<title>주간 리포트 ({html.escape(range_str)})</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,Pretendard,Segoe UI,Roboto,Apple SD Gothic Neo,Malgun Gothic,sans-serif; 
       margin:24px; line-height:1.5; color:#111}}
  h1{{font-size:20px; margin:0 0 12px}}
  h2{{font-size:16px; margin:28px 0 8px; border-top:1px solid #eee; padding-top:18px}}
  .box{{background:#fafafa; border:1px solid #eee; border-radius:10px; padding:14px}}
  pre{{white-space:pre-wrap; word-break:break-word; margin:0; font:13px/1.6 ui-monospace, Menlo, Consolas, monospace}}
  .meta{{color:#666; font-size:12px; margin-bottom:16px}}
</style>
</head><body>
<h1>주간 리포트 <span class="meta">({html.escape(range_str)})</span></h1>
""")
    for key, title in SRC_INFO:
        txt = read_text(f"slack_{key}.txt")
        # Slack 본문 그대로 보존(이모지/개행 포함)
        txt = html.escape(txt)
        parts.append(f"<h2>{html.escape(title)}</h2>")
        parts.append('<div class="box"><pre>')
        parts.append(txt)
        parts.append("</pre></div>")

    parts.append("</body></html>")
    return "\n".join(parts)

if __name__ == "__main__":
    range_str = find_range()
    # 파일명: weekly_YYYY-MM-DD_YYYY-MM-DD.html (range가 한 날짜면 그 날짜만)
    fname = ("weekly_" + range_str.replace("-", "_").replace("__", "_") + ".html").replace("__", "_")
    html_str = build_html(range_str)
    with open(fname, "w", encoding="utf-8") as f:
        f.write(html_str)
    # CI에서 path를 받기 위해 파일명만 출력
    print(fname)
