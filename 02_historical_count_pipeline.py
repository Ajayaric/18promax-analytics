
import os
import json
import csv
import time
import requests

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ==================================================
# 1. CONFIGURATION
# ==================================================

load_dotenv()

BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("X_BEARER_TOKEN missing from .env")

BASE_URL = "https://api.x.com/2/tweets/counts/all"

QUERY = "#18promax"
GRANULARITY = "day"

START_TIME = "2006-03-21T00:00:00Z"

# Maximum requests in one execution
MAX_PAGES_PER_RUN = 20

# Pause between requests
REQUEST_DELAY = 1.2

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = DATA_DIR / "02_checkpoint.json"
DATA_FILE = DATA_DIR / "02_historical_counts.jsonl"
AUDIT_FILE = DATA_DIR / "02_extraction_audit.csv"

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

# ==================================================
# 2. CHECKPOINT MANAGEMENT
# ==================================================

def save_checkpoint(checkpoint):

    temp_file = CHECKPOINT_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(checkpoint, file, indent=4)

    temp_file.replace(CHECKPOINT_FILE)


def load_checkpoint():

    if CHECKPOINT_FILE.exists():

        with open(
            CHECKPOINT_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            checkpoint = json.load(file)

        # Ensure we don't accidentally resume a different query
        if (
            checkpoint["query"] != QUERY
            or checkpoint["granularity"] != GRANULARITY
            or checkpoint["start_time"] != START_TIME
        ):
            raise ValueError(
                "Checkpoint configuration differs from current script. "
                "Back up the checkpoint before starting a new extraction."
            )

        print("Existing checkpoint found.")
        return checkpoint

    # First execution: establish a fixed end time
    end_time = (
        datetime.now(timezone.utc) - timedelta(minutes=2)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    checkpoint = {
        "query": QUERY,
        "granularity": GRANULARITY,
        "start_time": START_TIME,
        "end_time": end_time,
        "next_token": None,
        "pages_completed": 0,
        "total_records": 0,
        "total_posts": 0,
        "completed": False,
        "updated_at": None
    }

    save_checkpoint(checkpoint)

    print("New extraction initialized.")

    return checkpoint


# ==================================================
# 3. SAVE DAILY COUNT RECORDS
# ==================================================

def save_count_records(records):

    with open(
        DATA_FILE,
        "a",
        encoding="utf-8"
    ) as file:

        for record in records:

            output = {
                "query": QUERY,
                "granularity": GRANULARITY,
                "extracted_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                **record
            }

            file.write(
                json.dumps(output) + "\n"
            )


# ==================================================
# 4. AUDIT LOGGING
# ==================================================

def save_audit_log(
    page_number,
    records,
    page_total,
    first_bucket,
    last_bucket,
    next_page_available
):

    file_exists = AUDIT_FILE.exists()

    with open(
        AUDIT_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "page_number",
                "extracted_at",
                "daily_records",
                "posts_counted",
                "first_bucket",
                "last_bucket",
                "next_page_available"
            ]
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "page_number": page_number,
            "extracted_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "daily_records": records,
            "posts_counted": page_total,
            "first_bucket": first_bucket,
            "last_bucket": last_bucket,
            "next_page_available": next_page_available
        })


# ==================================================
# 5. FETCH HISTORICAL COUNTS
# ==================================================

def fetch_historical_counts():

    checkpoint = load_checkpoint()

    if checkpoint["completed"]:
        print("Historical extraction is already complete.")
        return

    pages_this_run = 0

    while pages_this_run < MAX_PAGES_PER_RUN:

        page_number = checkpoint["pages_completed"] + 1

        params = {
            "query": QUERY,
            "granularity": GRANULARITY,
            "start_time": checkpoint["start_time"],
            "end_time": checkpoint["end_time"]
        }

        if checkpoint["next_token"]:
            params["next_token"] = checkpoint["next_token"]

        print("\n" + "=" * 55)
        print(f"Fetching page: {page_number}")
        print(f"Run request: {pages_this_run + 1}/{MAX_PAGES_PER_RUN}")
        print("=" * 55)

        try:

            response = requests.get(
                BASE_URL,
                headers=HEADERS,
                params=params,
                timeout=60
            )

            print("HTTP Status:", response.status_code)

            if response.status_code != 200:

                print("API Error:", response.text)

                # Do not advance checkpoint on a failed request
                response.raise_for_status()

            result = response.json()

            records = result.get("data", [])
            metadata = result.get("meta", {})

            count_field = (
                "tweet_count"
                if records and "tweet_count" in records[0]
                else "post_count"
            )

            page_total = sum(
                int(record.get(count_field, 0))
                for record in records
            )

            first_bucket = (
                records[0].get("start")
                if records else None
            )

            last_bucket = (
                records[-1].get("end")
                if records else None
            )

            next_token = metadata.get("next_token")

            # --------------------------------------
            # SAVE DATA FIRST
            # --------------------------------------

            save_count_records(records)

            # --------------------------------------
            # SAVE AUDIT LOG
            # --------------------------------------

            save_audit_log(
                page_number,
                len(records),
                page_total,
                first_bucket,
                last_bucket,
                bool(next_token)
            )

            # --------------------------------------
            # UPDATE CHECKPOINT
            # --------------------------------------

            checkpoint["pages_completed"] += 1

            checkpoint["total_records"] += len(records)

            checkpoint["total_posts"] += page_total

            checkpoint["next_token"] = next_token

            checkpoint["updated_at"] = datetime.now(
                timezone.utc
            ).isoformat()

            if not next_token:
                checkpoint["completed"] = True

            save_checkpoint(checkpoint)

            pages_this_run += 1

            print("Daily records:", len(records))
            print("Posts counted:", page_total)
            print("First bucket:", first_bucket)
            print("Last bucket:", last_bucket)
            print("Next page available:", bool(next_token))

            print("Checkpoint saved successfully.")

            if checkpoint["completed"]:
                print("\nAll available historical count pages retrieved.")
                break

            time.sleep(REQUEST_DELAY)

        except requests.exceptions.RequestException as error:

            print("\nRequest failed:", error)
            print("Checkpoint retained for the next execution.")

            break

    print("\n" + "=" * 55)
    print("EXTRACTION RUN SUMMARY")
    print("=" * 55)

    print("Pages completed:", checkpoint["pages_completed"])
    print("Pages this run:", pages_this_run)
    print("Daily records:", checkpoint["total_records"])
    print("Total posts counted:", checkpoint["total_posts"])
    print("Extraction complete:", checkpoint["completed"])

    print("\nCheckpoint:", CHECKPOINT_FILE)
    print("Raw count data:", DATA_FILE)
    print("Audit log:", AUDIT_FILE)


# ==================================================
# 6. MAIN
# ==================================================

if __name__ == "__main__":

    print("=" * 55)
    print("18PROMAX RESUMABLE HISTORICAL COUNT PIPELINE")
    print("=" * 55)

    fetch_historical_counts()