# autopilot/html_editor.py
import re

def replace_slot(slot_name: str, new_content: str, html: str) -> str:
    pattern = re.compile(
        rf"(<!--\s*AUTOPILOT:{re.escape(slot_name)}:START\s*-->)(.*?)(<!--\s*AUTOPILOT:{re.escape(slot_name)}:END\s*-->)",
        re.DOTALL | re.IGNORECASE,
    )
    if not pattern.search(html):
        raise RuntimeError(f"Slot not found: {slot_name}")
    return pattern.sub(rf"\1\n{new_content}\n\3", html)

def apply_title_meta_slots(html_path: str, title: str, meta: str) -> bool:
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    new_html = html
    new_html = replace_slot("TITLE", f"<title>{title}</title>", new_html)
    new_html = replace_slot("META", f'<meta name="description" content="{meta}">', new_html)

    changed = (new_html != html)
    if changed:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(new_html)
    return changed
