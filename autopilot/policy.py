# autopilot/policy.py
from datetime import datetime, timezone

def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def days_since_last_change(state: dict) -> int:
    hist = state.get("history", [])
    for item in reversed(hist):
        if item.get("changed_html") is True and item.get("timestamp_utc"):
            last = _parse_ts(item["timestamp_utc"])
            now = datetime.now(timezone.utc)
            return (now - last).days
    return 999

def guardrail_check(days: int, guardrail_days: int, bypass: bool) -> None:
    if days < guardrail_days and not bypass:
        print(f"[AUTOPILOT] Guardrail activo: último cambio hace {days} días (< {guardrail_days}). Abortando.")
        raise SystemExit(0)

    if days < guardrail_days and bypass:
        print(f"[AUTOPILOT] BYPASS_GUARDRAIL=1 (testing). Último cambio hace {days} días, pero continúo.")
