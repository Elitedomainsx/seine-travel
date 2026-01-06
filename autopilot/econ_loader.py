# autopilot/econ_loader.py
import re
from typing import Optional

import pandas as pd


def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = (s.replace("á", "a").replace("é", "e").replace("í", "i")
           .replace("ó", "o").replace("ú", "u").replace("ñ", "n"))
    s = re.sub(r"\s+", " ", s)
    return s


def _find_col(cols, candidates):
    cols_n = [_norm(c) for c in cols]
    for cand in candidates:
        cn = _norm(cand)
        for i, c in enumerate(cols_n):
            if c == cn or cn in c:
                return cols[i]
    return None


def load_econ_clicks(csv_path: str) -> pd.DataFrame:
    """Load click logs from the Google Sheet export.

    Expected minimum columns (case-insensitive):
    - id (go link id)
    - ref (document.referrer)
    - timestamp/date/time (any parseable datetime)

    Returns a DF with standardized columns:
      ts (datetime64[ns, UTC])
      id (str)
      ref (str)
      ua (optional)
    """
    df = pd.read_csv(csv_path)
    if df.empty:
        return df

    id_col = _find_col(df.columns, ["id", "link_id", "go_id"])
    ref_col = _find_col(df.columns, ["ref", "referer", "referrer", "referencia"])
    ts_col = _find_col(df.columns, ["timestamp_utc", "timestamp", "time", "date", "datetime", "ts"])
    ua_col = _find_col(df.columns, ["ua", "user_agent", "agent"])

    missing = [k for k, v in [("id", id_col), ("ref", ref_col), ("timestamp", ts_col)] if v is None]
    if missing:
        raise RuntimeError(
            f"ECON CSV missing required columns: {missing}. Columns found: {list(df.columns)}. "
            "Fix the Google Sheet header row to include at least: timestamp, id, ref"
        )

    out = pd.DataFrame({
        "id": df[id_col].astype(str),
        "ref": df[ref_col].astype(str),
        "ts_raw": df[ts_col].astype(str),
    })
    if ua_col is not None:
        out["ua"] = df[ua_col].astype(str)

    # Parse timestamps. If tz-naive, assume UTC.
    ts = pd.to_datetime(out["ts_raw"], errors="coerce", utc=True)
    if ts.isna().all():
        # common Apps Script format: "Mon Jan 06 2026 10:05:00 GMT-0300 (Argentina Standard Time)"
        ts = pd.to_datetime(out["ts_raw"].str.replace("GMT", "GMT"), errors="coerce", utc=True)
    out["ts"] = ts
    out = out.drop(columns=["ts_raw"])
    out = out.dropna(subset=["ts"])
    return out


def count_outbound_clicks(
    clicks_df: pd.DataFrame,
    page_url: str,
    start_utc: Optional[pd.Timestamp] = None,
    end_utc: Optional[pd.Timestamp] = None,
):
    if clicks_df is None or clicks_df.empty:
        return {
            "outbound_clicks": 0,
            "by_id": {},
        }

    df = clicks_df
    if start_utc is not None:
        df = df[df["ts"] >= start_utc]
    if end_utc is not None:
        df = df[df["ts"] <= end_utc]

    # attribute clicks to a page via referrer substring match
    df = df[df["ref"].astype(str).str.contains(page_url, na=False)]

    total = int(len(df))
    by_id = df.groupby("id").size().sort_values(ascending=False).to_dict()
    by_id = {str(k): int(v) for k, v in by_id.items()}
    return {
        "outbound_clicks": total,
        "by_id": by_id,
    }
