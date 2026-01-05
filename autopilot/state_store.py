# autopilot/state_store.py
import json

STATE_PATH = "autopilot/state.json"

def load_state() -> dict:
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: dict) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def append_history(state: dict, item: dict) -> None:
    state.setdefault("history", []).append(item)

def set_template_index(state: dict, idx: int) -> None:
    state["current_template_index"] = int(idx)
