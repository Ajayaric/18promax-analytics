
import os
import json
import time
import sqlite3
import requests

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# =====================================================
# 1. CONFIGURATION
# =====================================================

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

# Maximum resources to retrieve across all executions
MAX_POSTS_TOTAL = 800

# Controlled batch execution
MAX_PAGES_PER_RUN = 2
PAGE_SIZE = 100

REQUEST_DELAY = 1.5
MAX_RETRIES = 3

POST_READ_COST = 0.005

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = DATA_DIR / "06_x_posts.sqlite"

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

POST_FIELDS = (
    "id,text,author_id,created_at,lang,"
    "public_metrics,entities,conversation_id,"
    "in_reply_to_user_id,referenced_posts"
)

# =====================================================
# 2. DATABASE INITIALIZATION
# =====================================================

def initialize_database():

    conn = sqlite3.connect(DB_FILE)

    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            post_id TEXT PRIMARY KEY,
            created_at TEXT,
            author_id TEXT,
            language TEXT,
            text TEXT,
            public_metrics_json TEXT,
            entities_json TEXT,
            conversation_id TEXT,
            raw_post_json TEXT NOT NULL,
            first_seen_page INTEGER,
            ingested_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_pages (
            page_number INTEGER PRIMARY KEY,
            fetched_at TEXT,
            result_count INTEGER,
            posts_returned INTEGER,
            next_token TEXT,
            raw_response_json TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_name TEXT PRIMARY KEY,
            query TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            next_token TEXT,
            pages_completed INTEGER DEFAULT 0,
            total_posts_returned INTEGER DEFAULT 0,
            total_unique_posts INTEGER DEFAULT 0,
            completed INTEGER DEFAULT 0,
            limit_reached INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)

    conn.commit()

    # Initialize a fixed extraction window only once
    state = conn.execute("""
        SELECT * FROM pipeline_state
        WHERE pipeline_name = '18promax_posts'
    """).fetchone()

    if state is None:

        end_time = (
            datetime.now(timezone.utc) - timedelta(minutes=2)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        conn.execute("""
            INSERT INTO pipeline_state (
                pipeline_name,
                query,
                start_time,
                end_time,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            "18promax_posts",
            QUERY,
            START_TIME,
            end_time,
            datetime.now(timezone.utc).isoformat()
        ))

        conn.commit()

        print("New extraction initialized.")

    conn.close()


# =====================================================
# 3. LOAD PIPELINE STATE
# =====================================================

def load_state(conn):

    conn.row_factory = sqlite3.Row

    row = conn.execute("""
        SELECT *
        FROM pipeline_state
        WHERE pipeline_name = '18promax_posts'
    """).fetchone()

    if row["query"] != QUERY or row["start_time"] != START_TIME:
        raise ValueError(
            "Existing database configuration does not match."
        )

    return dict(row)


# =====================================================
# 4. API REQUEST WITH RETRIES
# =====================================================

def fetch_page(params):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                URL,
                headers=HEADERS,
                params=params,
                timeout=60
            )

            if response.status_code == 429 or response.status_code >= 500:

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")

                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 2 ** attempt
                )

                print(
                    f"Temporary API issue. "
                    f"Retrying in {wait_seconds} seconds..."
                )

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException:

            if attempt == MAX_RETRIES:
                raise

            time.sleep(2 ** attempt)

    raise RuntimeError("Unable to retrieve API page.")


# =====================================================
# 5. ATOMIC PAGE PERSISTENCE
# =====================================================

def save_page(conn, page_number, result):

    posts = result.get("data", [])
    metadata = result.get("meta", {})

    next_token = metadata.get("next_token")

    fetched_at = datetime.now(timezone.utc).isoformat()

    # One transaction: raw response + posts + checkpoint
    with conn:

        for post in posts:

            metrics = post.get("public_metrics", {})
            entities = post.get("entities", {})

            conn.execute("""
                INSERT OR IGNORE INTO posts (
                    post_id,
                    created_at,
                    author_id,
                    language,
                    text,
                    public_metrics_json,
                    entities_json,
                    conversation_id,
                    raw_post_json,
                    first_seen_page,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post.get("id"),
                post.get("created_at"),
                post.get("author_id"),
                post.get("lang"),
                post.get("text"),
                json.dumps(metrics, ensure_ascii=False),
                json.dumps(entities, ensure_ascii=False),
                post.get("conversation_id"),
                json.dumps(post, ensure_ascii=False),
                page_number,
                fetched_at
            ))

        conn.execute("""
            INSERT OR REPLACE INTO raw_pages (
                page_number,
                fetched_at,
                result_count,
                posts_returned,
                next_token,
                raw_response_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            page_number,
            fetched_at,
            metadata.get("result_count", len(posts)),
            len(posts),
            next_token,
            json.dumps(result, ensure_ascii=False)
        ))

        total_returned = conn.execute("""
            SELECT COALESCE(SUM(posts_returned), 0)
            FROM raw_pages
        """).fetchone()[0]

        total_unique = conn.execute("""
            SELECT COUNT(*)
            FROM posts
        """).fetchone()[0]

        completed = 1 if not next_token else 0

        limit_reached = (
            1 if total_returned >= MAX_POSTS_TOTAL and next_token
            else 0
        )

        conn.execute("""
            UPDATE pipeline_state
            SET
                next_token = ?,
                pages_completed = ?,
                total_posts_returned = ?,
                total_unique_posts = ?,
                completed = ?,
                limit_reached = ?,
                updated_at = ?
            WHERE pipeline_name = '18promax_posts'
        """, (
            next_token,
            page_number,
            total_returned,
            total_unique,
            completed,
            limit_reached,
            fetched_at
        ))

    return len(posts), next_token


# =====================================================
# 6. MAIN EXTRACTION
# =====================================================

def run_extraction():

    initialize_database()

    conn = sqlite3.connect(DB_FILE)

    state = load_state(conn)

    if state["completed"]:
        print("All available posts have been retrieved.")
        conn.close()
        return

    if state["limit_reached"]:
        print("Maximum extraction limit reached.")
        conn.close()
        return

    pages_this_run = 0

    while pages_this_run < MAX_PAGES_PER_RUN:

        state = load_state(conn)

        remaining = (
            MAX_POSTS_TOTAL - state["total_posts_returned"]
        )

        if remaining < 10:

            print("Safety limit reached. Stopping extraction.")
            break

        page_number = state["pages_completed"] + 1

        max_results = min(PAGE_SIZE, remaining)

        params = {
            "query": QUERY,
            "start_time": state["start_time"],
            "end_time": state["end_time"],
            "max_results": max_results,
            "sort_order": "recency",
            "post.fields": POST_FIELDS
        }

        if state["next_token"]:
            params["next_token"] = state["next_token"]

        print("\n" + "=" * 55)
        print(f"Fetching page: {page_number}")
        print(f"Request: {pages_this_run + 1}/{MAX_PAGES_PER_RUN}")
        print("Maximum results:", max_results)
        print("Previously returned:", state["total_posts_returned"])
        print("=" * 55)

        try:

            result = fetch_page(params)

            posts_saved, next_token = save_page(
                conn,
                page_number,
                result
            )

            pages_this_run += 1

            state = load_state(conn)

            print("HTTP Status: 200")
            print("Posts returned:", posts_saved)
            print("Unique posts stored:", state["total_unique_posts"])
            print("Next page available:", bool(next_token))
            print("Database transaction committed.")

            estimated_cost = (
                state["total_posts_returned"] * POST_READ_COST
            )

            print(
                f"Estimated post-read cost: ${estimated_cost:.3f}"
            )

            if state["completed"]:
                print("\nAll available posts retrieved.")
                break

            if state["limit_reached"]:
                print("\nSafety limit reached.")
                break

            time.sleep(REQUEST_DELAY)

        except requests.exceptions.RequestException as error:

            print("\nAPI request failed:", error)
            print("No checkpoint advancement for failed page.")
            break

    state = load_state(conn)

    print("\n" + "=" * 55)
    print("POST EXTRACTION SUMMARY")
    print("=" * 55)

    print("Pages completed:", state["pages_completed"])
    print("Pages this run:", pages_this_run)
    print("Posts returned:", state["total_posts_returned"])
    print("Unique posts:", state["total_unique_posts"])
    print("Extraction complete:", bool(state["completed"]))
    print("Safety limit reached:", bool(state["limit_reached"]))

    print("\nSQLite database:", DB_FILE)

    conn.close()


# =====================================================
# 7. EXECUTION
# =====================================================

if __name__ == "__main__":

    print("=" * 55)
    print("18PROMAX FULL-ARCHIVE POST EXTRACTION")
    print("=" * 55)

    run_extraction()