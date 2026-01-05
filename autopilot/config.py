# autopilot/config.py
import json

CONFIG_PATH = "autopilot/config.json"

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)
