import os
import json
import requests

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ==========================================
# CONFIGURATION
# ==========================================

load_dotenv()

BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("X_BEARER_TOKEN missing from .env")

URL = "https://api.x.com/2/tweets/search/all"

QUERY = "#18promax"

START_TIME = "2025-01-01T00:00:00Z"
END_TIME = (
    datetime.now(timezone.utc) - timedelta(minutes=2)
).strftime("%Y-%m-%dT%H:%M:%SZ")

# Pilot only: retrieve a maximum of 10 posts
MAX_RESULTS = 10

PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "04_posts_pilot.json"

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

PARAMS = {
    "query": QUERY,
    "start_time": START_TIME,
    "end_time": END_TIME,
    "max_results": MAX_RESULTS,

    "post.fields": (
        "id,text,author_id,created_at,lang,"
        "public_metrics,entities,conversation_id,"
        "in_reply_to_user_id,referenced_posts"
    ),

    "expansions": "author_id",

    "user.fields": (
        "id,name,username,verified,public_metrics"
    )
}

# ==========================================
# API REQUEST
# ==========================================

print("=" * 50)
print("18PROMAX POST EXTRACTION PILOT")
print("=" * 50)

print("Query:", QUERY)
print("Start:", START_TIME)
print("End:", END_TIME)
print("Maximum posts:", MAX_RESULTS)

response = requests.get(
    URL,
    headers=HEADERS,
    params=PARAMS,
    timeout=60
)

print("\nHTTP Status:", response.status_code)

if response.status_code != 200:
    print("API Error:", response.text)
    response.raise_for_status()

result = response.json()

posts = result.get("data", [])
metadata = result.get("meta", {})

# ==========================================
# SAVE RAW API RESPONSE
# ==========================================

with open(
    OUTPUT_FILE,
    "w",
    encoding="utf-8"
) as file:

    json.dump(result, file, indent=4, ensure_ascii=False)

# ==========================================
# SUMMARY
# ==========================================

print("\nEXTRACTION SUMMARY")
print("-" * 50)

print("Posts retrieved:", len(posts))
print("Authors returned:", len(
    result.get("includes", {}).get("users", [])
))

print("Next page available:", bool(
    metadata.get("next_token")
))

print("Output file:", OUTPUT_FILE)

if posts:

    print("\nFIRST POST SAMPLE")

    sample = posts[0]

    print("Post ID:", sample.get("id"))
    print("Created at:", sample.get("created_at"))
    print("Author ID:", sample.get("author_id"))
    print("Language:", sample.get("lang"))
    print("Text:", sample.get("text"))

    print("Public metrics:")
    print(sample.get("public_metrics", {}))

print("\nPilot extraction completed.")