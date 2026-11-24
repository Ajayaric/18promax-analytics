import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta



from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# -----------------------------------------
# CONFIGURATION
# -----------------------------------------

load_dotenv()

BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("X_BEARER_TOKEN missing from .env")

URL = "https://api.x.com/2/tweets/counts/all"

QUERY = "#18promax"
GRANULARITY = "day"

START_TIME = "2006-03-21T00:00:00Z"

END_TIME = (
    datetime.now(timezone.utc) - timedelta(minutes=2)
).strftime("%Y-%m-%dT%H:%M:%SZ")

# Safety limit for our initial test
MAX_PAGES = 20

PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

# -----------------------------------------
# FETCH HISTORICAL COUNTS
# -----------------------------------------

def fetch_historical_counts():

    all_counts = []
    next_token = None
    page_number = 1

    while page_number <= MAX_PAGES:

        params = {
            "query": QUERY,
            "granularity": GRANULARITY,
            "start_time": START_TIME,
            "end_time": END_TIME
        }

        if next_token:
            params["next_token"] = next_token

        print(f"\nFetching page {page_number}...")
        print("Historical range:", START_TIME, "to", END_TIME)

        response = requests.get(
            URL,
            headers=HEADERS,
            params=params,
            timeout=60
        )

        print("HTTP Status:", response.status_code)

        if response.status_code != 200:
            print("API Error:", response.text)
            response.raise_for_status()

        result = response.json()

        page_data = result.get("data", [])

        all_counts.extend(page_data)

        metadata = result.get("meta", {})

        count_field = (
            "tweet_count"
            if page_data and "tweet_count" in page_data[0]
            else "post_count"
        )

        page_total = sum(
            int(row.get(count_field, 0))
            for row in page_data
        )

        print("Daily records:", len(page_data))
        print("Posts counted:", page_total)

        if page_data:
            print("First bucket:", page_data[0]["start"])
            print("Last bucket:", page_data[-1]["end"])

        next_token = metadata.get("next_token")

        print("Next page available:", bool(next_token))

        if not next_token:
            print("\nNo further pages available.")
            break

        page_number += 1
        time.sleep(1.2)

    return all_counts


# -----------------------------------------
# MAIN EXECUTION
# -----------------------------------------

if __name__ == "__main__":

    print("=" * 55)
    print("18PROMAX FULL-ARCHIVE COUNT EXTRACTION")
    print("=" * 55)

    counts = fetch_historical_counts()

    if not counts:
        print("No records returned.")
        raise SystemExit(0)

    df = pd.DataFrame(counts)

    count_field = (
        "tweet_count"
        if "tweet_count" in df.columns
        else "post_count"
    )

    df[count_field] = pd.to_numeric(df[count_field])

    df["start"] = pd.to_datetime(df["start"], utc=True)
    df["end"] = pd.to_datetime(df["end"], utc=True)

    df = df.drop_duplicates(
        subset=["start", "end"]
    ).sort_values("start")

    df.to_csv(
        OUTPUT_DIR / "18promax_historical_counts.csv",
        index=False
    )

    df.to_json(
        OUTPUT_DIR / "18promax_historical_counts.json",
        orient="records",
        date_format="iso",
        indent=4
    )

    print("\n" + "=" * 55)
    print("EXTRACTION SUMMARY")
    print("=" * 55)

    print("Hashtag:", QUERY)
    print("Daily records:", len(df))
    print("Total posts:", int(df[count_field].sum()))
    print("First date:", df["start"].min())
    print("Last date:", df["end"].max())

    print("\nExtraction completed!")