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

from datetime import timezone

def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def days_since_last_change(state: dict) -> int:
    hist = state.get("history", [])
    for item in reversed(hist):
        if item.get("changed_html") is True and item.get("timestamp_utc"):
            last = _parse_ts(item["timestamp_utc"])
            now = datetime.now(timezone.utc)
            return (now - last).days
    return 999  # nunca hubo cambios

GUARDRAIL_DAYS = int(cfg.get("guardrail_days", 14))
days = days_since_last_change(state)

if days < GUARDRAIL_DAYS:
    print(f"[AUTOPILOT] Guardrail activo: último cambio hace {days} días (< {GUARDRAIL_DAYS}). Abortando.")
    raise SystemExit(0)

def load_gsc_data(path: str):
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)

        # Esperamos CSV con columnas: page, query, impressions, clicks, ctr, position
        required = {"query", "impressions", "clicks", "ctr", "position"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"CSV missing columns: {missing}")

        # Agregamos por query (formato legacy esperado)
        df = df.groupby("query", as_index=False).agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            ctr=("ctr", "mean"),
            position=("position", "mean"),
        )
        return df

    # ---------- XLSX legacy (tal cual estaba) ----------
    xl = pd.ExcelFile(path)

    def norm(s: str) -> str:
        s = str(s).strip().lower()
        s = (s.replace("á","a").replace("é","e").replace("í","i")
               .replace("ó","o").replace("ú","u").replace("ñ","n"))
        s = re.sub(r"\s+", " ", s)
        return s

    preferred = None
    for sh in xl.sheet_names:
        if norm(sh) in ("consultas", "queries", "query"):
            preferred = sh
            break

    if preferred:
        df = pd.read_excel(path, sheet_name=preferred)
    else:
        candidate = None
        for sh in xl.sheet_names:
            tmp = pd.read_excel(path, sheet_name=sh, nrows=1)
            cols = [norm(c) for c in tmp.columns]
            if any(c in ("query", "consulta", "consultas") for c in cols):
                candidate = sh
                break
        if not candidate:
            raise RuntimeError(f"No sheet with queries found. Sheets: {xl.sheet_names}")
        df = pd.read_excel(path, sheet_name=candidate)

    return df

df = load_gsc_data(GSC_FILE)

print(f"[AUTOPILOT] Loaded GSC file: {GSC_FILE}")

def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    s = re.sub(r"\s+", " ", s)
    return s

df.columns = [norm(c) for c in df.columns]

# Map headers (ES/EN variants) -> canonical names
COLMAP = {
    "query": ["query", "consulta", "consultas", "consultas principales"],
    "impressions": ["impressions", "impresiones"],
    "clicks": ["clicks", "clics"],
    "ctr": ["ctr"],
    "position": ["position", "posicion", "posición"]
}

# Build reverse lookup
rename = {}
for canonical, variants in COLMAP.items():
    for v in variants:
        if v in df.columns:
            rename[v] = canonical
            break

df = df.rename(columns=rename)

required = {"query", "impressions", "clicks", "ctr", "position"}
missing = required - set(df.columns)
if missing:
    raise RuntimeError(f"GSC export missing columns after mapping: {missing}. Found: {list(df.columns)}")

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






