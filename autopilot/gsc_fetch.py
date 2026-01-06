import json
import os
from datetime import date, timedelta

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

DEFAULT_SITE_URL = "sc-domain:seine.travel"


def build_service():
    creds_json = os.environ["GSC_CREDENTIALS_JSON"]
    creds_info = json.loads(creds_json)

    credentials = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
    )
    return build("searchconsole", "v1", credentials=credentials)


def fetch_gsc_range(service, site_url: str, start_date: date, end_date: date, row_limit=25000):
    """Fetch page+query rows aggregated over a date range (inclusive)."""

    request = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "dimensions": ["page", "query"],
        "rowLimit": row_limit,
    }

    response = (
        service.searchanalytics()
        .query(siteUrl=site_url, body=request)
        .execute()
    )

    rows = []
    for row in response.get("rows", []):
        page, query = row["keys"][0], row["keys"][1]
        rows.append(
            {
                "page": page,
                "query": query,
                "impressions": row.get("impressions", 0),
                "clicks": row.get("clicks", 0),
                "ctr": row.get("ctr", 0.0),
                "position": row.get("position", 0.0),
                "startDate": request["startDate"],
                "endDate": request["endDate"],
            }
        )

    return pd.DataFrame(rows)


def fetch_gsc(service, site_url: str = DEFAULT_SITE_URL, days=28, row_limit=25000):
    """Backward compatible: fetch last N days ending yesterday."""
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days)
    return fetch_gsc_range(service, site_url=site_url, start_date=start_date, end_date=end_date, row_limit=row_limit)


def main():
    service = build_service()
    days = int(os.getenv("GSC_DAYS", "28"))
    site_url = os.getenv("GSC_SITE_URL", DEFAULT_SITE_URL)
    df = fetch_gsc(service, site_url=site_url, days=days)

    os.makedirs("data", exist_ok=True)
    df.to_csv("data/gsc_latest.csv", index=False)

    # pequeño “sanity print” para logs
    print(f"Rows: {len(df)} | Saved: data/gsc_latest.csv")
    if len(df) > 0:
        top = df.sort_values(["impressions", "clicks"], ascending=False).head(5)
        print("Top 5 by impressions/clicks:")
        print(top[["page", "query", "impressions", "clicks", "ctr", "position"]].to_string(index=False))


if __name__ == "__main__":
    main()
