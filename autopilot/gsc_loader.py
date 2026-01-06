# autopilot/gsc_loader.py
import pandas as pd
from pandas.errors import EmptyDataError
import re

def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    s = re.sub(r"\s+", " ", s)
    return s

def load_gsc_data(path: str, html_path: str, base_url: str):
    if path.lower().endswith(".csv"):
        try:
    df = pd.read_csv(path)
except EmptyDataError:
    # GSC can legitimately return 0 rows for very recent dates (data lag) or low-traffic periods.
    # Return an empty dataframe with the expected schema so the autopilot can HOLD safely.
    return pd.DataFrame(columns=["page", "query", "impressions", "clicks", "ctr", "position"])

        required = {"page", "query", "impressions", "clicks", "ctr", "position"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"CSV missing columns: {missing}")

        html = html_path.lstrip("./")
        if html in ("index.html", "/"):
            target_page = base_url
        else:
            target_page = f"{base_url}{html}"

        df = df[df["page"] == target_page].copy()
        print(f"[AUTOPILOT] CSV filtered to page: {target_page} | rows={len(df)}")

        if df.empty:
            raise RuntimeError(f"No GSC data for target page: {target_page}")

        df = df.groupby("query", as_index=False).agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            ctr=("ctr", "mean"),
            position=("position", "mean"),
        )
        return df

    # ---------- XLSX legacy (fallback) ----------
    xl = pd.ExcelFile(path)

    preferred = None
    for sh in xl.sheet_names:
        if _norm(sh) in ("consultas", "queries", "query"):
            preferred = sh
            break

    if preferred:
        df = pd.read_excel(path, sheet_name=preferred)
    else:
        candidate = None
        for sh in xl.sheet_names:
            tmp = pd.read_excel(path, sheet_name=sh, nrows=1)
            cols = [_norm(c) for c in tmp.columns]
            if any(c in ("query", "consulta", "consultas") for c in cols):
                candidate = sh
                break
        if not candidate:
            raise RuntimeError(f"No sheet with queries found. Sheets: {xl.sheet_names}")
        df = pd.read_excel(path, sheet_name=candidate)

    df.columns = [_norm(c) for c in df.columns]

    COLMAP = {
        "query": ["query", "consulta", "consultas", "consultas principales"],
        "impressions": ["impressions", "impresiones"],
        "clicks": ["clicks", "clics"],
        "ctr": ["ctr"],
        "position": ["position", "posicion", "posición"]
    }

    rename = {}
    for canonical, variants in COLMAP.items():
        for v in variants:
            if v in df.columns:
                rename[v] = canonical
                break

    df = df.rename(columns=rename)

    required = {"query", "impressions", "clicks", "ctr", "position"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"GSC export missing columns after mapping: {missing}. Found: {list(df.columns)}")

    return df
