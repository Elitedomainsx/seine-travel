import json
import os
from datetime import datetime
import pandas as pd

CSV_PATH = "data/gsc_latest.csv"
OUT_DIR = "decisions"
OUT_PATH = os.path.join(OUT_DIR, "actions.json")

THRESH = {
    "min_impr_snippet": 300,
    "max_ctr_snippet": 0.008,      # 0.8%
    "min_impr_pagequery": 80,
    "pos_low": 8.0,
    "pos_high": 20.0,
    "max_actions_total": 12,
    "max_actions_per_page": 3,
}

def load_gsc():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"No existe {CSV_PATH}. Primero corré gsc_fetch.py en el workflow.")
    df = pd.read_csv(CSV_PATH)

    df["ctr"] = pd.to_numeric(df.get("ctr", 0), errors="coerce").fillna(0.0)
    df["position"] = pd.to_numeric(df.get("position", 0), errors="coerce").fillna(0.0)
    df["impressions"] = pd.to_numeric(df.get("impressions", 0), errors="coerce").fillna(0).astype(int)
    df["clicks"] = pd.to_numeric(df.get("clicks", 0), errors="coerce").fillna(0).astype(int)
    df["page"] = df["page"].astype(str)
    df["query"] = df["query"].astype(str)
    return df

def build_actions(df: pd.DataFrame):
    actions = []
    per_page = {}

    # A) Snippet tests: páginas con impresiones altas y CTR bajo
    page_agg = df.groupby("page", as_index=False).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    )
    page_agg["ctr"] = page_agg.apply(
        lambda r: (r["clicks"] / r["impressions"]) if r["impressions"] > 0 else 0.0,
        axis=1,
    )

    cand_snippet = page_agg[
        (page_agg["impressions"] >= THRESH["min_impr_snippet"]) &
        (page_agg["ctr"] <= THRESH["max_ctr_snippet"])
    ].sort_values(["impressions", "ctr"], ascending=[False, True])

    for _, r in cand_snippet.iterrows():
        page = r["page"]
        actions.append({
            "type": "snippet_test",
            "page": page,
            "reason": {
                "impressions": int(r["impressions"]),
                "clicks": int(r["clicks"]),
                "ctr": float(r["ctr"]),
                "rule": "impressions_high_and_ctr_low"
            },
            "params": {"mode": "rewrite_title_meta", "variant_pool": "v1"}
        })
        per_page[page] = per_page.get(page, 0) + 1
        if len(actions) >= THRESH["max_actions_total"]:
            return actions

    # B) Content boosts: queries con potencial (posición 8–20)
    pq = df.groupby(["page", "query"], as_index=False).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        ctr=("ctr", "mean"),
        position=("position", "mean"),
    )

    cand_content = pq[
        (pq["impressions"] >= THRESH["min_impr_pagequery"]) &
        (pq["position"] >= THRESH["pos_low"]) &
        (pq["position"] <= THRESH["pos_high"])
    ].sort_values(["impressions", "position"], ascending=[False, True])

    for _, r in cand_content.iterrows():
        page = r["page"]
        if per_page.get(page, 0) >= THRESH["max_actions_per_page"]:
            continue
        if len(actions) >= THRESH["max_actions_total"]:
            break

        actions.append({
            "type": "content_boost",
            "page": page,
            "query": r["query"],
            "reason": {
                "impressions": int(r["impressions"]),
                "clicks": int(r["clicks"]),
                "ctr": float(r["ctr"]),
                "position": float(r["position"]),
                "rule": "position_8_20_and_impressions_ok"
            },
            "params": {"insert": ["h2_exact_match", "faq_block"], "variant_pool": "v1"}
        })
        per_page[page] = per_page.get(page, 0) + 1

    return actions

def main():
    df = load_gsc()
    actions = build_actions(df)

    os.makedirs(OUT_DIR, exist_ok=True)
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": {"csv": CSV_PATH},
        "thresholds": THRESH,
        "actions": actions
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Actions: {len(actions)} | Saved: {OUT_PATH}")

if __name__ == "__main__":
    main()
