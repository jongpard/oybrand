# -*- coding: utf-8 -*-
import yaml
from pathlib import Path

CONFIG_PATH = Path("config/report_format.yaml")

def load_cfg():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def make_link(text: str, url: str | None, target: str = "slack") -> str:
    if not url:
        return text
    if target == "slack":
        return f"<{url}|{text}>"
    return f'<a href="{url}">{text}</a>'

def inline_list(items) -> str:
    # 가로 인라인 출력 (중복 제거 + 정렬)
    items = [str(x).strip() for x in items if str(x).strip()]
    return ", ".join(sorted(set(items)))
