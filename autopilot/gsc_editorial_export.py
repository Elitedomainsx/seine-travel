"""Read-only Search Console export for the Seine.Travel editorial calendar.

The workflow uploads these files as an Actions artifact. It never edits site HTML
or commits private search data to the public source repository.
"""

import csv
import json
import os
from datetime import date, timedelta, timezone, datetime
from pathlib import Path

from autopilot.gsc_fetch import build_service


SITE_URL = os.getenv("GSC_SITE_URL", "sc-domain:seine.travel")
OUTPUT = Path(os.getenv("GSC_OUTPUT_DIR", "gsc-editorial-export"))
PAGE_SIZE = 25000
FIELDS = ["page", "query", "clicks", "impressions", "ctr", "position", "startDate", "endDate"]


def query_rows(service, start, end, dimensions):
    result = []
    offset = 0
    while True:
        body = {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "dimensions": dimensions,
            "type": "web",
            "rowLimit": PAGE_SIZE,
            "startRow": offset,
        }
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=body).execute()
        batch = response.get("rows", [])
        for item in batch:
            keys = dict(zip(dimensions, item.get("keys", [])))
            result.append({
                "page": keys.get("page", ""),
                "query": keys.get("query", ""),
                "clicks": item.get("clicks", 0),
                "impressions": item.get("impressions", 0),
                "ctr": item.get("ctr", 0),
                "position": item.get("position", 0),
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
            })
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset >= 50000:
            raise RuntimeError("Search Console row cap reached; split the date range before using an incomplete export")
    return result


def write_csv(name, rows):
    with (OUTPUT / name).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    if not os.getenv("GSC_CREDENTIALS_JSON"):
        raise RuntimeError("GSC_CREDENTIALS_JSON is missing")
    service = build_service()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    end = date.today() - timedelta(days=3)
    manifest = {
        "property": SITE_URL,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_end_date": end.isoformat(),
        "source": "Google Search Console Search Analytics API",
        "note": "API rows may omit anonymized queries and are not an exhaustive list of searches.",
        "windows": {},
    }
    for days in (28, 90):
        start = end - timedelta(days=days - 1)
        counts = {}
        for label, dimensions in (
            ("queries", ["query"]),
            ("pages", ["page"]),
            ("page_queries", ["page", "query"]),
        ):
            rows = query_rows(service, start, end, dimensions)
            write_csv(f"{days}d_{label}.csv", rows)
            counts[label] = len(rows)
        manifest["windows"][str(days)] = {"start": start.isoformat(), "end": end.isoformat(), "rows": counts}
    (OUTPUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
