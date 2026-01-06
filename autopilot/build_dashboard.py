# autopilot/build_dashboard.py
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fmt(x: Any, nd: int = 2) -> str:
    if x is None:
        return "-"
    try:
        if isinstance(x, (int,)):
            return f"{x}"
        if isinstance(x, float):
            if abs(x) >= 1000:
                return f"{x:,.0f}"
            return f"{x:.{nd}f}"
        return str(x)
    except Exception:
        return str(x)


def _pct_change(cur: float, prev: float) -> Optional[float]:
    try:
        if prev == 0:
            return None
        return (cur - prev) / prev
    except Exception:
        return None


def _spark(values: List[float], width: int = 140, height: int = 28) -> str:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return ""
    mn, mx = min(vals), max(vals)
    if mx == mn:
        mx = mn + 1e-9
    pts = []
    for i, v in enumerate(values):
        x = int(i * (width - 2) / (len(values) - 1)) + 1
        y = int((1 - (v - mn) / (mx - mn)) * (height - 2)) + 1
        pts.append(f"{x},{y}")
    poly = " ".join(pts)
    return f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="2" points="{poly}"/></svg>'


def _iso_to_dt(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def build_dashboard(
    cycles_path: str = "autopilot/logs/cycles.jsonl",
    state_path: str = "autopilot/state.json",
    output_html: str = "dashboard/index.html",
    output_md: str = "autopilot/logs/latest_cycle.md",
) -> None:
    cycles = _read_jsonl(cycles_path)
    state = _read_json(state_path) or {}

    latest = cycles[-1] if cycles else {}
    prev = cycles[-2] if len(cycles) >= 2 else {}

    m = (latest.get("metrics") or {})
    mp = (prev.get("metrics") or {})

    def metric(name: str, default: float = 0.0) -> float:
        try:
            return float(m.get(name, default))
        except Exception:
            return default

    def metric_prev(name: str, default: float = 0.0) -> float:
        try:
            return float(mp.get(name, default))
        except Exception:
            return default

    kpis = [
        ("Impressions", "impressions", 0),
        ("GSC Clicks", "gsc_clicks", 0),
        ("Outbound clicks", "outbound_clicks", 0),
        ("Outbound / 1k impr", "outbound_per_1k_impr", 2),
        ("CTR", "gsc_ctr", 4),
        ("Avg position", "gsc_position", 2),
    ]

    # Build trend arrays (last 26 cycles)
    tail = cycles[-26:]
    trend = {
        "impressions": [float((c.get("metrics") or {}).get("impressions", 0.0)) for c in tail],
        "outbound_per_1k_impr": [float((c.get("metrics") or {}).get("outbound_per_1k_impr", 0.0)) for c in tail],
        "outbound_clicks": [float((c.get("metrics") or {}).get("outbound_clicks", 0.0)) for c in tail],
        "gsc_clicks": [float((c.get("metrics") or {}).get("gsc_clicks", 0.0)) for c in tail],
    }

    # Variant leaderboard
    variants = state.get("variants", {}) or {}
    leaderboard = []
    for k, v in variants.items():
        s = (v.get("score_agg") or {})
        leaderboard.append({
            "variant_key": k,
            "intent": (k.split("#")[0] if "#" in k else v.get("intent", "")),
            "n_obs": int(s.get("n_obs", 0)),
            "impressions": float(s.get("impressions", 0.0)),
            "outbound_clicks": float(s.get("outbound_clicks", 0.0)),
            "outbound_per_1k_impr": float(s.get("outbound_per_1k_impr", 0.0)),
            "gsc_clicks": float(s.get("gsc_clicks", 0.0)),
        })
    leaderboard.sort(key=lambda r: (r["outbound_per_1k_impr"], r["gsc_clicks"]), reverse=True)
    best_key = state.get("best_variant_key")

    # Health checks
    now = datetime.now(timezone.utc)
    last_ts = _iso_to_dt(latest.get("timestamp_utc", "")) if latest else None
    stale_days = None
    if last_ts:
        stale_days = (now - last_ts).days

    next_eta = None
    if last_ts:
        next_eta = last_ts + timedelta(days=7)
    else:
        next_eta = now + timedelta(days=7)

    # Build MD summary
    os.makedirs(os.path.dirname(output_md) or ".", exist_ok=True)
    with open(output_md, "w", encoding="utf-8") as fmd:
        fmd.write(f"# Autopilot latest cycle\n\n")
        fmd.write(f"- Timestamp (UTC): {latest.get('timestamp_utc','-')}\n")
        fmd.write(f"- Window: {latest.get('window',{}).get('start','-')} → {latest.get('window',{}).get('end','-')}\n")
        fmd.write(f"- Action: **{latest.get('action','-')}**\n")
        fmd.write(f"- Active variant: `{(latest.get('chosen') or {}).get('variant_key','-')}`\n")
        fmd.write(f"- Best variant: `{best_key or '-'}`\n")
        fmd.write("\n## KPIs (latest window)\n")
        for label, key, nd in kpis:
            fmd.write(f"- {label}: {_fmt(metric(key), nd)}\n")

    # HTML dashboard
    os.makedirs(os.path.dirname(output_html) or ".", exist_ok=True)

    def card(label: str, key: str, nd: int) -> str:
        cur = metric(key)
        prv = metric_prev(key)
        d = _pct_change(cur, prv)
        d_txt = "—" if d is None else f"{d*100:+.1f}%"
        return f"""
        <div class="card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-val">{_fmt(cur, nd)}</div>
          <div class="kpi-delta">{d_txt}</div>
        </div>
        """

    kpi_cards = "\n".join(card(*k) for k in kpis)

    trend_row = f"""
      <div class="trend">
        <div class="trend-item">
          <div class="trend-title">Outbound / 1k impr</div>
          <div class="spark">{_spark(trend["outbound_per_1k_impr"])}</div>
        </div>
        <div class="trend-item">
          <div class="trend-title">Outbound clicks</div>
          <div class="spark">{_spark(trend["outbound_clicks"])}</div>
        </div>
        <div class="trend-item">
          <div class="trend-title">Impressions</div>
          <div class="spark">{_spark(trend["impressions"])}</div>
        </div>
      </div>
    """

    # Cycles table
    def row(c: Dict[str, Any]) -> str:
        mm = c.get("metrics") or {}
        chosen = c.get("chosen") or {}
        ts = c.get("timestamp_utc", "-")
        w = c.get("window") or {}
        wtxt = f'{w.get("start","-")} → {w.get("end","-")}'
        return f"""
        <tr>
          <td class="mono">{ts}</td>
          <td class="mono">{wtxt}</td>
          <td>{c.get("action","-")}</td>
          <td class="mono">{chosen.get("variant_key","-")}</td>
          <td>{_fmt(mm.get("impressions",0.0),0)}</td>
          <td>{_fmt(mm.get("gsc_clicks",0.0),0)}</td>
          <td>{_fmt(mm.get("outbound_clicks",0.0),0)}</td>
          <td>{_fmt(mm.get("outbound_per_1k_impr",0.0),2)}</td>
          <td>{_fmt(mm.get("gsc_ctr",0.0),4)}</td>
          <td>{_fmt(mm.get("gsc_position",0.0),2)}</td>
        </tr>
        """

    cycles_rows = "\n".join(row(c) for c in cycles[-30:][::-1])

    
    # Top queries rows (latest cycle)
    topq = latest.get("top_queries") or []
    topq_rows = ""
    for q in topq[:12]:
        topq_rows += f"""<tr>
          <td>{str(q.get('query',''))}</td>
          <td>{_fmt(q.get('impressions',0.0),0)}</td>
          <td>{_fmt(q.get('clicks',0.0),0)}</td>
          <td>{_fmt(q.get('ctr',0.0),4)}</td>
          <td>{_fmt(q.get('position',0.0),2)}</td>
        </tr>"""

# Leaderboard table
    lb_rows = ""
    for r in leaderboard[:12]:
        cls = "best" if r["variant_key"] == best_key else ""
        lb_rows += f"""
        <tr class="{cls}">
          <td class="mono">{r["variant_key"]}</td>
          <td>{r["intent"]}</td>
          <td>{r["n_obs"]}</td>
          <td>{_fmt(r["impressions"],0)}</td>
          <td>{_fmt(r["outbound_clicks"],0)}</td>
          <td>{_fmt(r["outbound_per_1k_impr"],2)}</td>
          <td>{_fmt(r["gsc_clicks"],0)}</td>
        </tr>
        """

    warn = ""
    if stale_days is not None and stale_days >= 14:
        warn = f'<div class="warn">⚠️ Latest cycle is {stale_days} days old. Autopilot may be broken or schedule paused.</div>'

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <meta name="robots" content="noindex,nofollow"/>
  <title>Seine.travel Autopilot Dashboard</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #0b1020;
      --card: rgba(255,255,255,0.06);
      --border: rgba(255,255,255,0.12);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.7);
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    header {{
      padding: 18px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 14px;
      flex-wrap: wrap;
    }}
    h1 {{ font-size: 18px; margin: 0; }}
    .sub {{ color: var(--muted); font-size: 13px; }}
    .wrap {{ padding: 20px; max-width: 1100px; margin: 0 auto; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }}
    @media (max-width: 900px) {{
      .cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 600px) {{
      .cards {{ grid-template-columns: 1fr; }}
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px 14px;
    }}
    .kpi-label {{ color: var(--muted); font-size: 12px; }}
    .kpi-val {{ font-size: 26px; margin-top: 4px; }}
    .kpi-delta {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; }}
    .section-title {{ margin: 18px 0 10px; font-size: 14px; color: var(--muted); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      overflow: hidden;
    }}
    th, td {{
      padding: 10px 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      font-size: 13px;
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-weight: 600; background: rgba(255,255,255,0.03); }}
    tr:last-child td {{ border-bottom: none; }}
    .best td {{ outline: 1px solid rgba(255,255,255,0.25); }}
    .trend {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    @media (max-width: 900px) {{
      .trend {{ grid-template-columns: 1fr; }}
    }}
    .trend-item {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 10px 12px;
    }}
    .trend-title {{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    .spark {{ color: rgba(255,255,255,0.9); }}
    .warn {{
      margin: 12px 0;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid rgba(255,170,0,0.5);
      background: rgba(255,170,0,0.10);
      color: rgba(255,255,255,0.9);
    }}
    .meta {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }}
    .meta strong {{ color: var(--text); }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Seine.travel — Autopilot Dashboard</h1>
      <div class="sub">Last cycle: <span class="mono">{latest.get("timestamp_utc","-")}</span> · Next ETA (approx): <span class="mono">{next_eta.isoformat().replace("+00:00","Z")}</span></div>
    </div>
    <div class="sub">
      Active: <span class="mono">{(latest.get("chosen") or {}).get("variant_key","-")}</span> · Best: <span class="mono">{best_key or "-"}</span> · Action: <strong>{latest.get("action","-")}</strong>
    </div>
  </header>

  <div class="wrap">
    {warn}

    <div class="cards">
      {kpi_cards}
    </div>

    {trend_row}

    <div class="meta">
      <div><strong>Window:</strong> <span class="mono">{(latest.get("window") or {}).get("start","-")}</span> → <span class="mono">{(latest.get("window") or {}).get("end","-")}</span></div>
      <div><strong>Dominant intent (queries):</strong> {latest.get("dominant_intent","-")}</div>
      <div><strong>Chosen title:</strong> {((latest.get("chosen") or {}).get("title")) or "-"}</div>
      <div><strong>Chosen meta:</strong> {((latest.get("chosen") or {}).get("meta")) or "-"}</div>
    </div>


    <div class="section-title">Top queries (latest window)</div>
    <table>
      <thead>
        <tr>
          <th>Query</th><th>Impr</th><th>Clicks</th><th>CTR</th><th>Pos</th>
        </tr>
      </thead>
      <tbody>
        {topq_rows}
      </tbody>
    </table>

    <div class="section-title">Variant leaderboard (aggregated)</div>
    <table>
      <thead>
        <tr>
          <th>Variant</th><th>Intent</th><th>Obs</th><th>Impr</th><th>Outbound</th><th>Outbound/1k</th><th>GSC clicks</th>
        </tr>
      </thead>
      <tbody>
        {lb_rows}
      </tbody>
    </table>

    <div class="section-title">Cycles (latest 30)</div>
    <table>
      <thead>
        <tr>
          <th>Timestamp (UTC)</th><th>Window</th><th>Action</th><th>Variant</th><th>Impr</th><th>GSC clicks</th><th>Outbound</th><th>Outbound/1k</th><th>CTR</th><th>Pos</th>
        </tr>
      </thead>
      <tbody>
        {cycles_rows}
      </tbody>
    </table>

    <div class="section-title">Notes</div>
    <div class="meta">
      <div>• Dashboard is <strong>noindex</strong> (not meant for SEO).</div>
      <div>• Primary signal: <strong>Outbound clicks per 1k impressions</strong> (from econ sensor).</div>
      <div>• Rollback triggers when current variant underperforms best variant by configured drop% (and enough data).</div>
    </div>
  </div>
</body>
</html>
"""
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)