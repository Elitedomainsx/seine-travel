import json
import os
from typing import List, Optional

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from autopilot.config import load_config


def _build_sheets_service(creds_json: str):
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=credentials)


def _pick_sheet_name(sheet_titles: List[str]) -> str:
    """Try to choose the most likely 'logs' tab; fallback to the first tab."""
    if not sheet_titles:
        raise RuntimeError("No sheets found in spreadsheet")

    preferred = [
        "logs",
        "log",
        "events",
        "clicks",
        "click",
        "data",
    ]
    low = {t.lower().strip(): t for t in sheet_titles}
    for p in preferred:
        for k, original in low.items():
            if p == k or p in k:
                return original
    return sheet_titles[0]


def fetch_econ_sheet_to_csv(
    spreadsheet_id: str,
    output_csv: str,
    creds_json: str,
    sheet_name: Optional[str] = None,
    max_cols: str = "Z",
):
    service = _build_sheets_service(creds_json)

    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    chosen = sheet_name or _pick_sheet_name(titles)

    rng = f"{chosen}!A1:{max_cols}"
    resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])

    if not values:
        raise RuntimeError(f"No values returned from spreadsheet range: {rng}")

    header = values[0]
    rows = values[1:]
    # If the first row doesn't look like headers, keep it as data.
    header_l = [str(h).strip().lower() for h in header]
    looks_like_header = any(k in header_l for k in ["id", "ref", "referer", "timestamp", "time", "date"])
    if not looks_like_header:
        # synth headers
        rows = values
        header = [f"col_{i}" for i in range(len(values[0]))]

    df = pd.DataFrame(rows, columns=header)

    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    df.to_csv(output_csv, index=False)

    print(f"[ECON] Saved {len(df)} rows to {output_csv} (sheet='{chosen}')")
    if len(df) > 0:
        print("[ECON] Head:")
        print(df.head(3).to_string(index=False))


def main():
    cfg = load_config()
    spreadsheet_id = cfg.get("econ_spreadsheet_id")
    output_csv = cfg.get("econ_csv", "data/econ_clicks_latest.csv")
    if not spreadsheet_id:
        raise RuntimeError("Missing 'econ_spreadsheet_id' in autopilot/config.json")

    creds_json = os.environ.get("GSC_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Missing env var GSC_CREDENTIALS_JSON (used for Sheets read scope as well)")

    fetch_econ_sheet_to_csv(spreadsheet_id=spreadsheet_id, output_csv=output_csv, creds_json=creds_json)


if __name__ == "__main__":
    main()
