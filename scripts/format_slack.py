# -*- coding: utf-8 -*-
import json
from pathlib import Path
from format_utils import load_cfg, make_link, inline_list

CFG = load_cfg()
ARW = CFG["arrows"]
LBL = CFG["labels"]

SRC_ORDER = ["oy_kor","oy_global","amazon_us","qoo10_jp","daiso_kr"]

def line_top10(item, target="slack"):
    raw = item.get("raw") or item.get("raw_name") or item.get("name") or ""
    url = item.get("url")
    days = item.get("keep_days", 0)
    avg_rank = item.get("avg_rank")
    delta = item.get("delta")  # + / - / 0 (optional)

    arrow = ARW["flat"]
    if isinstance(delta, (int,float)):
        arrow = ARW["up"] if delta < 0 else (ARW["down"] if delta > 0 else ARW["flat"])

    raw_fmt = make_link(raw, url, target=target) if CFG["slack"]["link"] else raw
    tail = ""
    if CFG["trend"]["parentheses"]:
        if avg_rank is not None:
            tail = f" (유지 {days}일 · 평균 {avg_rank:.1f}위)"
        else:
            tail = f" (유지 {days}일)"
    return f"{raw_fmt} {arrow}{tail}".strip()

def block_stats(s):
    topn = s.get("topn", 100)
    uniq = s.get("unique_cnt", 0)
    keep = s.get("keep_days_mean", 0.0)
    return (
        f"{CFG['labels']['stats']}\n"
        f"- {LBL['top_count']}: {uniq}개\n"
        f"- {LBL['keep_mean']}: {keep:.1f}일"
    )

def block_keywords(s):
    kw = s.get("kw", {}) or {}
    parts = []
    for key in ["product_type","benefits","marketing","ingredients","influencers"]:
        vals = kw.get(key, [])
        if not vals:
            continue
        title = {
            "product_type": "제품형태",
            "benefits": "효능",
            "marketing": "마케팅",
            "ingredients": "성분",
            "influencers": "인플루언서",
        }[key]
        parts.append(f"- {title}: {inline_list(vals)}")
    return "\n".join(parts)

def build_src_text(src, sdict):
    title = sdict.get("title", src)
    rng = sdict.get("range", "")
    items = sdict.get("top10_items", [])[:10]

    lines = [f"📈 주간 리포트 · {title} ({rng})", "🏆 Top10"]
    if not items:
        lines.append("데이터 없음")
    else:
        for i, it in enumerate(items, start=1):
            lines.append(f"{i}. {line_top10(it, target='slack')}")

    # 통계
    lines.append("")
    lines.append(block_stats(sdict))

    # 키워드(인라인)
    kw_block = block_keywords(sdict)
    if kw_block:
        lines.extend(["", "🔎 주간 키워드 분석", kw_block])

    # 히어로/반짝(세로)
    heroes = sdict.get("heroes") or []
    flashes = sdict.get("flashes") or []
    if heroes:
        lines.extend(["", "🔥 히어로(3일 이상 랭크 유지):"])
        for it in heroes:
            lines.append(f"- {line_top10(it, target='slack')}")
    if flashes:
        lines.extend(["", "✨ 반짝 아이템(2일 이내 랭크 아웃):"])
        for it in flashes:
            lines.append(f"- {line_top10(it, target='slack')}")

    return "\n".join(lines)

def main():
    outdir = Path("dist")
    outdir.mkdir(parents=True, exist_ok=True)
    summary = json.loads(Path("weekly_summary.json").read_text(encoding="utf-8"))

    for src in SRC_ORDER:
        if src not in summary:
            continue
        text = build_src_text(src, summary[src])
        (outdir / f"slack_{src}.txt").write_text(text, encoding="utf-8")

if __name__ == "__main__":
    main()
