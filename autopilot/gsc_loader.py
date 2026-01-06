# autopilot/gsc_loader.py
import pandas as pd
from pandas.errors import EmptyDataError
import re

EXPECTED = ["query", "impressions", "clicks", "ctr", "position"]

def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    s = re.sub(r"\s+", " ", s)
    return s

def _empty_df():
    return pd.DataFrame(columns=EXPECTED)

def load_gsc_data(path: str, html_path: str, base_url: str) -> pd.DataFrame:
    # ---- CSV path (new) ----
    if str(path).lower().endswith(".csv"):
        try:
            df = pd.read_csv(path)
        except FileNotFoundError:
            return _empty_df()
        except EmptyDataError:
            # CSV vacío es válido (lag de GSC / poco tráfico)
            return _empty_df()

        if df is None or df.empty:
            return _empty_df()

        required = {"page", "query", "impressions", "clicks", "ctr", "position"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

        html = str(html_path).lstrip("./")
        if html in ("index.html", "/"):
            target_page = base_url
        else:
            target_page = f"{base_url}{html}"

        df = df[df["page"] == target_page].copy()
        print(f"[AUTOPILOT] CSV filtered to page: {target_page} | rows={len(df)}")

        # Si no hay filas, NO rompemos: devolvemos vacío para que el autopilot haga HOLD.
        if df.empty:
            return _empty_df()

        out = df.groupby("query", as_index=False).agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            ctr=("ctr", "mean"),
            position=("position", "mean"),
        )

        # asegurar esquema
        for c in EXPECTED:
            if c not in out.columns:
                out[c] = None
        return out[EXPECTED]

    # ---- XLSX legacy (fallback) ----
    try:
        xl = pd.ExcelFile(path)
    except FileNotFoundError:
        return _empty_df()

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

    if df is None or df.empty:
        return _empty_df()

    df.columns = [_norm(c) for c in df.columns]

    COLMAP = {
        "query": ["query", "consulta", "consultas", "consultas principales"],
        "impressions": ["impressions", "impresiones"],
        "clicks": ["clicks", "clics"],
        "ctr": ["ctr"],
        "position": ["position", "posicion", "posición"],
    }

    rename = {}
    for canonical, variants in COLMAP.items():
        for v in variants:
            if v in df.columns:
                rename[v] = canonical
                break

    df = df.rename(columns=rename)

    missing = set(EXPECTED) - set(df.columns)
    if missing:
        raise RuntimeError(f"GSC export missing columns after mapping: {missing}. Found: {list(df.columns)}")

    return df[EXPECTED]
