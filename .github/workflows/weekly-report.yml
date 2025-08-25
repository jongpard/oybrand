# -*- coding: utf-8 -*-
import os, json, html, re
from datetime import datetime

SRC_INFO = [
    ("oy_kor",    "올리브영 국내 Top100"),
    ("oy_global", "올리브영 글로벌 Top100"),
    ("amazon_us", "아마존 US Top100"),
    ("qoo10_jp",  "큐텐 재팬 뷰티 Top200"),
    ("daiso_kr",  "다이소몰 뷰티/위생 Top200"),
]

URL_RE = re.compile(r'(https?://[^\s]+)')

def read_text(p):
    return open(p, 'r', encoding='utf-8').read() if os.path.exists(p) else "데이터 없음"

def find_range():
    for key, _ in SRC_INFO:
        p = f"weekly_summary_{key}.json"
        if os.path.exists(p):
            try:
                r = json.load(open(p, "r", encoding="utf-8")).get("range")
                if r: return r
            except Exception:
                pass
    return datetime.utcnow().strftime("%Y-%m-%d")

def slack_to_html(text: str) -> str:
    # 1) HTML 이스케이프
    s = html.escape(text)
    # 2) *볼드* 변환
    s = re.sub(r'\*(.+?)\*', r'<b>\1</b>', s)
    # 3) URL 자동 링크
    s = URL_RE.sub(lambda m: f'<a href="{m.group(1)}" target="_blank">{m.group(1)}</a>', s)
    # 4) 개행 보존
    s = s.replace('\n', '<br>')
    return s

def build_html(range_str: str) -> str:
    parts = [f"""<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"/>
<title>주간 리포트 ({html.escape(range_str)})</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,Pretendard,Segoe UI,Roboto,Apple SD Gothic Neo,Malgun Gothic,sans-serif;
       margin:24px; line-height:1.6; color:#111}}
  h1{{font-size:22px; margin:0 0 12px}}
  h2{{font-size:16px; margin:28px 0 8px; border-top:1px solid #eee; padding-top:18px}}
  .box{{background:#fafafa; border:1px solid #eee; border-radius:10px; padding:14px}}
</style>
</head><body>
<h1>주간 리포트 <small>({html.escape(range_str)})</small></h1>
"""]
    for key, title in SRC_INFO:
        txt = read_text(f"slack_{key}.txt")
        parts.append(f"<h2>{html.escape(title)}</h2>")
        parts.append(f'<div class="box">{slack_to_html(txt)}</div>')
    parts.append("</body></html>")
    return "\n".join(parts)

if __name__ == "__main__":
    r = find_range()
    fname = ("weekly_" + r.replace("-", "_").replace("__","_") + ".html")
    open(fname, "w", encoding="utf-8").write(build_html(r))
    print(fname)
