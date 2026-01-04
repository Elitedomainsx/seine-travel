import pandas as pd
import json
import re
from datetime import datetime

CONFIG_PATH = "autopilot/config.json"
STATE_PATH = "autopilot/state.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

HTML_PATH = cfg["html_path"]
GSC_FILE = cfg["gsc_file"]
MIN_IMPRESSIONS = int(cfg.get("min_impressions", 50))

# --- Load state ---
with open(STATE_PATH, "r", encoding="utf-8") as f:
    state = json.load(f)

# --- Load GSC export ---
df = pd.read_excel(GSC_FILE)
df.columns = [c.strip().lower() for c in df.columns]

required = {"query", "impressions", "clicks", "ctr", "position"}
missing = required - set(df.columns)
if missing:
    raise RuntimeError(f"GSC export missing columns: {missing}")

total_impr = float(df["impressions"].sum())
if total_impr < MIN_IMPRESSIONS:
    print(f"Not enough impressions ({total_impr}). Exiting without changes.")
    raise SystemExit(0)

# --- Intent scoring (deterministic) ---
intent_scores = {"comparison": 0.0, "price": 0.0, "trust": 0.0}
for _, row in df.iterrows():
    q = str(row["query"]).lower()
    w = float(row["impressions"])
    if re.search(r"\b(best|top|which)\b", q):
        intent_scores["comparison"] += w
    if re.search(r"\b(price|cost|cheap)\b", q):
        intent_scores["price"] += w
    if re.search(r"\b(review|reviews|worth)\b", q):
        intent_scores["trust"] += w

dominant_intent = max(intent_scores, key=intent_scores.get)

# --- Variables (v0.2 simple; later derive from queries) ---
topic = "Seine River Cruises"
city = "Paris"
year = "2026"

TEMPLATES = {
    "comparison": [
        ("Best {topic} in {city} ({year}) – Prices & Reviews",
         "Compare the best {topic} in {city} by price, route and experience. See which option is worth booking."),
        ("Which Is the Best {topic}? Honest {city} Comparison",
         "Not sure which {topic} to choose? We compare top Paris options to help you decide."),
        ("Best {topic} – Compare Routes, Dining & Prices",
         "Looking for the best {topic}? Compare Paris cruises by route, dining and value.")
    ],
    "price": [
        ("{topic} Prices in {city} ({year}) – What You’ll Actually Pay",
         "See typical {topic} prices in {city}, what’s included, and which options offer the best value."),
        ("Best Value {topic} in {city} – Prices & What’s Included",
         "Compare {topic} options by price and inclusions to pick the best-value cruise for your trip."),
        ("Cheap vs Premium {topic} in {city} – Price Comparison",
         "A clear price comparison of {topic} options in {city}, from budget to premium experiences.")
    ],
    "trust": [
        ("Best {topic} in {city} – Reviews & What Travelers Say ({year})",
         "Compare {topic} options in {city} using real review signals and practical differences to choose confidently."),
        ("Is a {topic} Worth It? {city} Reviews & Comparison",
         "See what travelers love (and dislike) about {topic} in {city} and choose the right option."),
        ("Top-Rated {topic} in {city} ({year}) – Honest Picks",
         "A curated comparison of top-rated {topic} in {city}, focused on what matters for your booking.")
    ]
}

templates = TEMPLATES[dominant_intent]
idx = int(state.get("current_template_index", 0)) % len(templates)
title_tpl, meta_tpl = templates[idx]

title = title_tpl.format(topic=topic, city=city, year=year)
meta = meta_tpl.format(topic=topic, city=city, year=year)

def replace_slot(slot_name: str, new_content: str, html: str) -> str:
    pattern = re.compile(
        rf"(<!--\s*AUTOPILOT:{re.escape(slot_name)}:START\s*-->)(.*?)(<!--\s*AUTOPILOT:{re.escape(slot_name)}:END\s*-->)",
        re.DOTALL | re.IGNORECASE,
    )
    if not pattern.search(html):
        raise RuntimeError(f"Slot not found: {slot_name}")
    return pattern.sub(rf"\1\n{new_content}\n\3", html)

# --- Load HTML and apply ---
with open(HTML_PATH, "r", encoding="utf-8") as f:
    html = f.read()

new_html = html
new_html = replace_slot("TITLE", f"<title>{title}</title>", new_html)
new_html = replace_slot("META", f'<meta name="description" content="{meta}">', new_html)

changed = (new_html != html)
if changed:
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(new_html)

# --- Update state regardless (so we rotate templates each run) ---
state["current_template_index"] = (idx + 1) % len(templates)
state.setdefault("history", []).append({
    "timestamp_utc": datetime.utcnow().isoformat() + "Z",
    "dominant_intent": dominant_intent,
    "template_index_used": idx,
    "title": title,
    "meta": meta,
    "total_impressions_in_export": total_impr,
    "changed_html": changed
})

with open(STATE_PATH, "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2)

print(f"Done. intent={dominant_intent}, template={idx}, changed={changed}")
