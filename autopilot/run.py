# autopilot/run.py
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os

import pandas as pd

from autopilot.config import load_config
from autopilot.state_store import load_state, save_state, append_history
from autopilot.policy import days_since_last_change, guardrail_check
from autopilot.gsc_fetch import build_service as build_gsc_service, fetch_gsc_range
from autopilot.gsc_loader import load_gsc_data
from autopilot.intent import detect_dominant_intent
from autopilot.templates import build_templates, pick_template_for_intent
from autopilot.html_editor import apply_title_meta_slots
from autopilot.build_dashboard import build_dashboard
from autopilot.econ_fetch import fetch_econ_sheet_to_csv
from autopilot.econ_loader import load_econ_clicks, count_outbound_clicks
from autopilot.scoring import update_variant_aggregates, choose_best_variant, choose_best_intent
from autopilot.cycle_log import utc_now_iso, append_jsonl, write_json


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _last_change_ts(state: dict) -> datetime | None:
    hist = state.get("history", [])
    for item in reversed(hist):
        if item.get("changed_html") is True and item.get("timestamp_utc"):
            return _parse_ts(item["timestamp_utc"])
    return None


def _ensure_state_schema(state: dict, cfg: dict) -> dict:
    # Minimal, backward compatible migration.
    state.setdefault("schema_version", 2)
    state.setdefault("variants", {})
    state.setdefault("best_variant_key", None)
    state.setdefault("active_variant_key", None)

    if state.get("active_variant") is None:
        baseline = state.get("baseline", {})
        state["active_variant"] = {
            "variant_key": state.get("active_variant_key") or "baseline",
            "intent": "baseline",
            "template_index": int(state.get("current_template_index", 0)),
            "title": baseline.get("title", ""),
            "meta": baseline.get("meta", ""),
            "applied_at_utc": _last_change_ts(state).isoformat().replace("+00:00", "Z") if _last_change_ts(state) else utc_now_iso(),
        }

    # Track per-intent indices (so switching intent does not scramble rotation)
    state.setdefault("template_index_by_intent", {})
    return state


def _eval_window(state: dict, cfg: dict) -> tuple[date, date]:
    """Compute a stable evaluation window:
    - start = day after last HTML change
    - end = yesterday
    This avoids mixing metrics across two variants in the same window.
    """
    end = date.today() - timedelta(days=1)
    last = _last_change_ts(state)
    if not last:
        # fallback: last 28 days
        start = end - timedelta(days=28)
        return start, end
    start = (last.date() + timedelta(days=1))
    if start > end:
        start = end
    return start, end


def _variant_key(intent: str, idx: int) -> str:
    return f"{intent}#{int(idx)}"




def _intent_agg(variants: dict, intent: str) -> dict:
    total_impr = 0.0
    total_out = 0.0
    total_gsc = 0.0
    n_obs = 0
    for k, v in (variants or {}).items():
        k_intent = k.split("#")[0] if "#" in k else (v.get("intent") or "")
        if k_intent != intent:
            continue
        s = (v.get("score_agg") or {})
        total_impr += float(s.get("impressions", 0.0))
        total_out += float(s.get("outbound_clicks", 0.0))
        total_gsc += float(s.get("gsc_clicks", 0.0))
        n_obs += int(s.get("n_obs", 0))
    out_1k = 1000.0 * total_out / total_impr if total_impr > 0 else 0.0
    return {"intent": intent, "impressions": total_impr, "outbound_clicks": total_out, "gsc_clicks": total_gsc, "n_obs": n_obs, "outbound_per_1k_impr": out_1k}
def main():
    cfg = load_config()

    base_url = cfg.get("base_url", "https://seine.travel/")
    html_path = cfg["html_path"]
    gsc_file = cfg.get("gsc_file", "data/gsc_latest.csv")
    gsc_site_url = cfg.get("gsc_site_url", "sc-domain:seine.travel")
    econ_sheet_id = cfg.get("econ_spreadsheet_id")
    econ_csv = cfg.get("econ_csv", "data/econ_clicks_latest.csv")

    min_impressions_to_change = int(cfg.get("min_impressions_to_change", 50))
    guardrail_days = int(cfg.get("guardrail_days", 14))

    eval_min_impressions = int(cfg.get("eval_min_impressions", 120))
    eval_min_outbound = int(cfg.get("eval_min_outbound_clicks", 2))
    rollback_drop_pct = float(cfg.get("rollback_drop_pct", 0.25))

    prefer_best_intent = bool(cfg.get("prefer_best_intent", True))
    intent_switch_margin_pct = float(cfg.get("intent_switch_margin_pct", 0.15))
    best_intent_min_outbound = float(cfg.get("best_intent_min_outbound_clicks", 3))

    topic = cfg.get("topic", "Seine River Cruises")
    city = cfg.get("city", "Paris")
    year = cfg.get("year", str(date.today().year))

    # ---------------- state + guardrail ----------------
    state = load_state()
    state = _ensure_state_schema(state, cfg)

    days = days_since_last_change(state)
    bypass = os.environ.get("BYPASS_GUARDRAIL", "0") == "1"
    can_change = True
    try:
        guardrail_check(days, guardrail_days, bypass)
    except SystemExit:
        can_change = False

    # ---------------- evaluation window ----------------
    start_d, end_d = _eval_window(state, cfg)

    # ---------------- fetch GSC for that window ----------------
    gsc_service = build_gsc_service()
    df_gsc_raw = fetch_gsc_range(gsc_service, site_url=gsc_site_url, start_date=start_d, end_date=end_d)
    os.makedirs("data", exist_ok=True)
    df_gsc_raw.to_csv(gsc_file, index=False)
    print(f"[GSC] Saved {len(df_gsc_raw)} rows to {gsc_file} ({start_d}..{end_d})")

    # Load and filter to target page
    df_page = load_gsc_data(path=gsc_file, html_path=html_path, base_url=base_url)
    total_impr = float(df_page["impressions"].sum())
    total_clicks = float(df_page["clicks"].sum())
    avg_ctr = float(df_page["ctr"].mean()) if len(df_page) else 0.0
    avg_pos = float(df_page["position"].mean()) if len(df_page) else 0.0

    # Top queries snapshot (for dashboard/debug)
    try:
        top_queries = (
            df_page.sort_values("impressions", ascending=False)
            .head(8)[["query", "impressions", "clicks", "ctr", "position"]]
            .to_dict("records")
        )
    except Exception:
        top_queries = []


    # ---------------- fetch ECON clicks ----------------
    econ_ok = False
    econ_total = 0
    econ_by_id = {}
    if econ_sheet_id:
        creds_json = os.environ.get("GSC_CREDENTIALS_JSON")
        if not creds_json:
            raise RuntimeError("Missing env var GSC_CREDENTIALS_JSON (needed for Sheets read scope)")
        fetch_econ_sheet_to_csv(spreadsheet_id=econ_sheet_id, output_csv=econ_csv, creds_json=creds_json)
        econ_df = load_econ_clicks(econ_csv)

        page_url = base_url.rstrip("/") + "/" + html_path.lstrip("/")
        start_ts = pd.Timestamp(datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc))
        end_ts = pd.Timestamp(datetime.combine(end_d, datetime.max.time(), tzinfo=timezone.utc))
        econ = count_outbound_clicks(econ_df, page_url=page_url, start_utc=start_ts, end_utc=end_ts)
        econ_total = int(econ["outbound_clicks"])
        econ_by_id = econ.get("by_id", {})
        econ_ok = True
        print(f"[ECON] Outbound clicks for {page_url} in window: {econ_total}")
    else:
        print("[ECON] No econ_spreadsheet_id configured. Skipping econ fetch.")

    # ---------------- intent + candidate selection ----------------
    dominant_intent = detect_dominant_intent(df_page)
    templates = build_templates(topic=topic, city=city, year=year)

    # Choose next template index for that intent
    idx_by_intent = state.setdefault("template_index_by_intent", {})
    current_idx = int(idx_by_intent.get(dominant_intent, 0)) % len(templates[dominant_intent])

    # ---------------- score current active variant ----------------
    active = state.get("active_variant", {})
    active_key = active.get("variant_key") or state.get("active_variant_key") or "baseline"

    # store observation against active variant (de-dup by window)
    v = state.setdefault("variants", {}).setdefault(active_key, {})
    obs = v.setdefault("observations", [])
    new_obs = {
        "window": {"start": start_d.isoformat(), "end": end_d.isoformat()},
        "metrics": {
            "impressions": total_impr,
            "gsc_clicks": total_clicks,
            "gsc_ctr": avg_ctr,
            "gsc_position": avg_pos,
            "outbound_clicks": econ_total,
            "outbound_by_id": econ_by_id,
            "outbound_per_1k_impr": (1000.0 * econ_total / total_impr) if total_impr > 0 else 0.0,
        },
    }
    replaced = False
    for i in range(len(obs) - 1, -1, -1):
        if obs[i].get("window") == new_obs["window"]:
            obs[i] = new_obs
            replaced = True
            break
    if not replaced:
        obs.append(new_obs)

    update_variant_aggregates(state)
    best_key = choose_best_variant(state.get("variants", {}))
    state["best_variant_key"] = best_key

    # Current vs best for rollback
    variants = state.get("variants", {})
    cur_score = (variants.get(active_key, {}) or {}).get("score_agg", {})
    best_score = (variants.get(best_key, {}) or {}).get("score_agg", {}) if best_key else {}

    cur_out_1k = float(cur_score.get("outbound_per_1k_impr", 0.0))
    best_out_1k = float(best_score.get("outbound_per_1k_impr", 0.0))
    cur_out = float(cur_score.get("outbound_clicks", 0.0))
    best_out = float(best_score.get("outbound_clicks", 0.0))
    cur_impr = float(cur_score.get("impressions", 0.0))

    enough_to_eval = (cur_impr >= eval_min_impressions) and (cur_out >= eval_min_outbound)
    best_is_better = (best_key is not None) and (best_key != active_key) and (best_out_1k > 0)

    do_rollback = False
    if can_change and enough_to_eval and best_is_better:
        drop = 1.0 - (cur_out_1k / best_out_1k) if best_out_1k > 0 else 0.0
        if drop >= rollback_drop_pct and best_out >= eval_min_outbound:
            do_rollback = True

    action = "hold"
    chosen_intent = dominant_intent
    chosen_idx = current_idx
    chosen_title = None
    chosen_meta = None
    changed = False

    if total_impr < min_impressions_to_change:
        action = "hold_not_enough_impressions"
        can_change = False

    if can_change:
        if do_rollback and best_key:
            # Best key format: intent#idx (or 'baseline')
            action = "rollback"
            if "#" in best_key:
                chosen_intent, idx_s = best_key.split("#", 1)
                chosen_idx = int(idx_s)
                chosen_title, chosen_meta = pick_template_for_intent(templates[chosen_intent], chosen_idx)
            else:
                # baseline rollback
                chosen_title = state.get("baseline", {}).get("title", "")
                chosen_meta = state.get("baseline", {}).get("meta", "")
        else:
            action = "explore"

            # Prefer an intent that has proven better ECON performance, but only when evidence exists.
            if prefer_best_intent:
                intent_best = choose_best_intent(state.get("variants", {}))
                if intent_best and intent_best.get("intent") and intent_best.get("intent") != dominant_intent:
                    if float(intent_best.get("outbound_clicks", 0.0)) >= best_intent_min_outbound:
                        dom_stats = _intent_agg(state.get("variants", {}), dominant_intent)
                        dom_out_1k = float(dom_stats.get("outbound_per_1k_impr", 0.0))
                        best_out_1k = float(intent_best.get("outbound_per_1k_impr", 0.0))
                        if dom_out_1k <= 0 and best_out_1k > 0:
                            chosen_intent = intent_best["intent"]
                        elif best_out_1k > dom_out_1k * (1.0 + intent_switch_margin_pct):
                            chosen_intent = intent_best["intent"]

            # Recompute index for the chosen intent (pool is tiny: 2 templates per intent)
            chosen_idx = int(idx_by_intent.get(chosen_intent, 0)) % len(templates[chosen_intent])
            chosen_title, chosen_meta = pick_template_for_intent(templates[chosen_intent], chosen_idx)

        if chosen_title is not None and chosen_meta is not None:
            changed = apply_title_meta_slots(html_path=html_path, title=chosen_title, meta=chosen_meta)

            # advance index only when exploring (rollback keeps the winner)
            if action == "explore":
                idx_by_intent[chosen_intent] = (chosen_idx + 1) % len(templates[chosen_intent])
            else:
                idx_by_intent[chosen_intent] = int(chosen_idx)

            # update active variant
            new_key = _variant_key(chosen_intent, chosen_idx) if action != "hold" else active_key
            state["active_variant_key"] = new_key
            state["active_variant"] = {
                "variant_key": new_key,
                "intent": chosen_intent,
                "template_index": int(chosen_idx),
                "title": chosen_title,
                "meta": chosen_meta,
                "applied_at_utc": utc_now_iso(),
            }

    # ---------------- history + logs ----------------
    cycle = {
        "timestamp_utc": utc_now_iso(),
        "window": {"start": start_d.isoformat(), "end": end_d.isoformat()},
        "page": html_path,
        "dominant_intent": dominant_intent,
        "top_queries": top_queries,
        "action": action,
        "can_change": can_change,
        "chosen": {
            "intent": chosen_intent,
            "template_index": int(chosen_idx),
            "variant_key": state.get("active_variant_key"),
            "changed_html": changed,
            "title": chosen_title,
            "meta": chosen_meta,
        },
        "metrics": {
            "impressions": total_impr,
            "gsc_clicks": total_clicks,
            "gsc_ctr": avg_ctr,
            "gsc_position": avg_pos,
            "econ_enabled": econ_ok,
            "outbound_clicks": econ_total,
            "outbound_by_id": econ_by_id,
            "outbound_per_1k_impr": (1000.0 * econ_total / total_impr) if total_impr > 0 else 0.0,
        },
        "best_variant_key": best_key,
        "scores": {
            "current_outbound_per_1k_impr": cur_out_1k,
            "best_outbound_per_1k_impr": best_out_1k,
        },
    }

    append_history(state, {"timestamp_utc": cycle["timestamp_utc"], "action": action, "changed_html": changed, **cycle})
    save_state(state)

    # observability artifacts
    append_jsonl("autopilot/logs/cycles.jsonl", cycle)
    write_json("autopilot/logs/latest_cycle.json", cycle)
    # dashboard + human-readable summary
    build_dashboard(
        cycles_path="autopilot/logs/cycles.jsonl",
        state_path="autopilot/state.json",
        output_html="dashboard/index.html",
        output_md="autopilot/logs/latest_cycle.md",
    )

    print(f"Done. action={action} intent={dominant_intent} can_change={can_change} changed={changed}")


if __name__ == "__main__":
    main()
