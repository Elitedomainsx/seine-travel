# autopilot/run.py
from datetime import datetime, timezone
import os

from autopilot.config import load_config
from autopilot.state_store import load_state, save_state, append_history, set_template_index
from autopilot.policy import days_since_last_change, guardrail_check
from autopilot.gsc_loader import load_gsc_data
from autopilot.intent import detect_dominant_intent
from autopilot.templates import build_templates, pick_template_for_intent
from autopilot.html_editor import apply_title_meta_slots


def main():
    cfg = load_config()

    HTML_PATH = cfg["html_path"]
    GSC_FILE = cfg["gsc_file"]
    MIN_IMPRESSIONS = int(cfg.get("min_impressions", 50))
    GUARDRAIL_DAYS = int(cfg.get("guardrail_days", 14))

    state = load_state()

    # --- Guardrail ---
    days = days_since_last_change(state)
    bypass = os.environ.get("BYPASS_GUARDRAIL", "0") == "1"
    guardrail_check(days, GUARDRAIL_DAYS, bypass)

    # --- Load GSC export (csv/xlsx) ---
    df = load_gsc_data(
        path=GSC_FILE,
        html_path=HTML_PATH,
        base_url="https://seine.travel/",
    )
    print(f"[AUTOPILOT] Loaded GSC file: {GSC_FILE}")

    total_impr = float(df["impressions"].sum())
    if total_impr < MIN_IMPRESSIONS:
        print(f"Not enough impressions ({total_impr}). Exiting without changes.")
        raise SystemExit(0)

    # --- Decide intent ---
    dominant_intent = detect_dominant_intent(df)

    # --- Templates (same as current behavior) ---
    templates = build_templates(topic="Seine River Cruises", city="Paris", year="2026")
    chosen_set = templates[dominant_intent]

    idx = int(state.get("current_template_index", 0)) % len(chosen_set)
    title, meta = pick_template_for_intent(chosen_set, idx)

    # --- Apply to HTML slots ---
    changed = apply_title_meta_slots(
        html_path=HTML_PATH,
        title=title,
        meta=meta,
    )

    # --- State update (NO-OP refactor: same behavior as before: rotate always) ---
    set_template_index(state, (idx + 1) % len(chosen_set))
    append_history(
        state=state,
        item={
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "dominant_intent": dominant_intent,
            "template_index_used": idx,
            "title": title,
            "meta": meta,
            "total_impressions_in_export": total_impr,
            "changed_html": changed,
        },
    )
    save_state(state)

    print(f"Done. intent={dominant_intent}, template={idx}, changed={changed}")


if __name__ == "__main__":
    main()







