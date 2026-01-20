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


def _sanitize_header(header: List[object]) -> List[str]:
    """Make sure we always have non-empty, unique column names."""
    cleaned: List[str] = []
    seen = set()
    for i, h in enumerate(header):
        name = str(h).strip() if h is not None else ""
        if not name:
            name = f"col_{i+1}"
        # Ensure uniqueness
        base = name
        j = 2
        while name in seen:
            name = f"{base}_{j}"
            j += 1
        seen.add(name)
        cleaned.append(name)
    return cleaned


def _normalize_rows(rows: List[list], ncols: int) -> tuple[list, int, int, int]:
    """Pad/truncate ragged rows so pandas doesn't explode.

    Google Sheets API omits trailing empty cells, which creates ragged rows.
    """
    fixed: list = []
    padded = truncated = skipped_empty = 0

    for r in rows:
        if r is None:
            continue

        # Skip totally empty rows
        if not any(str(x).strip() for x in r):
            skipped_empty += 1
            continue

        if len(r) < ncols:
            padded += 1
            r = r + [""] * (ncols - len(r))
        elif len(r) > ncols:
            truncated += 1
            r = r[:ncols]

        fixed.append(r)

    return fixed, padded, truncated, skipped_empty


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
        rows = values
        header = [f"col_{i+1}" for i in range(len(values[0]))]

    header = _sanitize_header(header)

    # IMPORTANT: Normalize ragged rows (Sheets API omits trailing empty cells)
    n = len(header)
    fixed_rows, padded, truncated, skipped_empty = _normalize_rows(rows, n)
    if padded or truncated or skipped_empty:
        print(
            f"[ECON] Normalized rows: padded={padded}, truncated={truncated}, "
            f"skipped_empty={skipped_empty}, expected_cols={n}"
        )

    df = pd.DataFrame(fixed_rows, columns=header)

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
