from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Score:
    impressions: float
    gsc_clicks: float
    outbound_clicks: float

    @property
    def outbound_per_1k_impr(self) -> float:
        if self.impressions <= 0:
            return 0.0
        return 1000.0 * float(self.outbound_clicks) / float(self.impressions)


def choose_best_variant(variants: Dict[str, dict]) -> Optional[str]:
    """Pick best variant by outbound_per_1k_impr (primary) then gsc_clicks."""
    best_key = None
    best_score = (-1.0, -1.0)
    for k, v in (variants or {}).items():
        s = v.get("score_agg", {})
        o = float(s.get("outbound_per_1k_impr", 0.0))
        g = float(s.get("gsc_clicks", 0.0))
        tup = (o, g)
        if tup > best_score:
            best_score = tup
            best_key = k
    return best_key


def update_variant_aggregates(state: dict) -> None:
    """Aggregate observations per variant into a stable score."""
    variants = state.setdefault("variants", {})
    for k, v in variants.items():
        obs = v.get("observations", [])
        total_impr = 0.0
        total_gsc_clicks = 0.0
        total_outbound = 0.0
        for o in obs:
            m = o.get("metrics", {})
            total_impr += float(m.get("impressions", 0.0))
            total_gsc_clicks += float(m.get("gsc_clicks", 0.0))
            total_outbound += float(m.get("outbound_clicks", 0.0))

        outbound_per_1k = 0.0
        if total_impr > 0:
            outbound_per_1k = 1000.0 * total_outbound / total_impr

        v["score_agg"] = {
            "impressions": total_impr,
            "gsc_clicks": total_gsc_clicks,
            "outbound_clicks": total_outbound,
            "outbound_per_1k_impr": outbound_per_1k,
            "n_obs": len(obs),
        }


def choose_best_intent(variants: Dict) -> Optional[dict]:
    """Aggregate variant scores by intent and return best intent summary.

    Returns dict with keys: intent, outbound_per_1k_impr, outbound_clicks, impressions, n_obs
    """
    if not variants:
        return None

    agg = {}
    for k, v in variants.items():
        intent = (v.get("intent") or "").strip() or (k.split("#")[0] if "#" in k else "unknown")
        s = v.get("score_agg") or {}
        agg.setdefault(intent, {"impressions": 0.0, "outbound_clicks": 0.0, "gsc_clicks": 0.0, "n_obs": 0})
        agg[intent]["impressions"] += float(s.get("impressions", 0.0))
        agg[intent]["outbound_clicks"] += float(s.get("outbound_clicks", 0.0))
        agg[intent]["gsc_clicks"] += float(s.get("gsc_clicks", 0.0))
        agg[intent]["n_obs"] += int(s.get("n_obs", 0))

    best = None
    for intent, a in agg.items():
        impr = a["impressions"]
        out = a["outbound_clicks"]
        out_1k = 1000.0 * out / impr if impr > 0 else 0.0
        row = {"intent": intent, "outbound_per_1k_impr": out_1k, "outbound_clicks": out, "impressions": impr, "n_obs": a["n_obs"], "gsc_clicks": a["gsc_clicks"]}
        if best is None:
            best = row
        else:
            if row["outbound_per_1k_impr"] > best["outbound_per_1k_impr"]:
                best = row
            elif row["outbound_per_1k_impr"] == best["outbound_per_1k_impr"] and row["gsc_clicks"] > best["gsc_clicks"]:
                best = row
    return best
