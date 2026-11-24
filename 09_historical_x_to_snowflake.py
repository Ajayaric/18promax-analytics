
import os
import json
import time
import uuid
import requests
import snowflake.connector

from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ==========================================
# 1. PROJECT CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("X_BEARER_TOKEN missing from .env")

URL = "https://api.x.com/2/tweets/search/all"

QUERY = "#18promax"

START_TIME = "2025-01-01T00:00:00Z"

END_TIME = (
    datetime.now(timezone.utc) - timedelta(minutes=2)
).strftime("%Y-%m-%dT%H:%M:%SZ")

MAX_RESULTS = 100
MAX_PAGES_PER_RUN = 2

PIPELINE_NAME = "X_18PROMAX_HISTORICAL"

DATABASE = "TWITTER_ANALYTICS"
BRONZE_SCHEMA = "BRONZE"
CONTROL_SCHEMA = "CONTROL"

BATCH_ID = str(uuid.uuid4())

TARGET_TABLE = (
    f"{DATABASE}.{BRONZE_SCHEMA}.RAW_X_POSTS"
)

AUDIT_TABLE = (
    f"{DATABASE}.{BRONZE_SCHEMA}.RAW_API_PAGES"
)

STATE_TABLE = (
    f"{DATABASE}.{CONTROL_SCHEMA}.EXTRACTION_STATE"
)

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}"
}

print("=" * 55)
print("18PROMAX HISTORICAL X-TO-SNOWFLAKE PIPELINE")
print("=" * 55)

print("Pipeline:", PIPELINE_NAME)
print("Query:", QUERY)
print("Start:", START_TIME)
print("End:", END_TIME)
print("Results per page:", MAX_RESULTS)
print("Maximum pages per run:", MAX_PAGES_PER_RUN)
print("Batch ID:", BATCH_ID)


# ==========================================
# 2. SNOWFLAKE CONNECTION
# ==========================================

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "authenticator": "snowflake",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": DATABASE,
    "schema": BRONZE_SCHEMA,
    "role": os.getenv("SNOWFLAKE_ROLE")
}

SNOWFLAKE_CONFIG = {
    key: value
    for key, value in SNOWFLAKE_CONFIG.items()
    if value
}

required_fields = [
    "account",
    "user",
    "password",
    "warehouse"
]

missing_fields = [
    field for field in required_fields
    if field not in SNOWFLAKE_CONFIG
]

if missing_fields:
    raise ValueError(
        f"Missing Snowflake configuration: {missing_fields}"
    )


# ==========================================
# 3. CONNECT TO SNOWFLAKE
# ==========================================

print("\nConnecting to Snowflake...")

conn = snowflake.connector.connect(
    **SNOWFLAKE_CONFIG,
    autocommit=False
)

cursor = conn.cursor()

print("Snowflake connection successful!")


# ==========================================
# 4. CHECKPOINT INITIALIZATION
# ==========================================

def format_snowflake_timestamp(value):

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


print("\nChecking extraction checkpoint...")

cursor.execute(f"""
    SELECT
        PIPELINE_NAME,
        QUERY_TEXT,
        START_TIME,
        END_TIME,
        NEXT_TOKEN,
        PAGES_COMPLETED,
        TOTAL_POSTS_RETURNED,
        TOTAL_UNIQUE_POSTS,
        IS_COMPLETE
    FROM {STATE_TABLE}
    WHERE PIPELINE_NAME = %s
      AND QUERY_TEXT = %s
    ORDER BY UPDATED_AT DESC
    LIMIT 1
""", (PIPELINE_NAME, QUERY))

checkpoint = cursor.fetchone()


# ==========================================
# 5. CREATE INITIAL CHECKPOINT IF REQUIRED
# ==========================================

if checkpoint is None:

    print("No existing checkpoint found.")
    print("Creating a new extraction checkpoint...")

    cursor.execute(f"""
        INSERT INTO {STATE_TABLE} (
            PIPELINE_NAME,
            QUERY_TEXT,
            START_TIME,
            END_TIME,
            NEXT_TOKEN,
            PAGES_COMPLETED,
            TOTAL_POSTS_RETURNED,
            TOTAL_UNIQUE_POSTS,
            IS_COMPLETE
        )
        VALUES (
            %s,
            %s,
            TO_TIMESTAMP_TZ(%s),
            TO_TIMESTAMP_TZ(%s),
            NULL,
            0,
            0,
            0,
            FALSE
        )
    """, (
        PIPELINE_NAME,
        QUERY,
        START_TIME,
        END_TIME
    ))

    conn.commit()

    print("Initial checkpoint created successfully.")

    cursor.execute(f"""
        SELECT
            PIPELINE_NAME,
            QUERY_TEXT,
            START_TIME,
            END_TIME,
            NEXT_TOKEN,
            PAGES_COMPLETED,
            TOTAL_POSTS_RETURNED,
            TOTAL_UNIQUE_POSTS,
            IS_COMPLETE
        FROM {STATE_TABLE}
        WHERE PIPELINE_NAME = %s
          AND QUERY_TEXT = %s
        ORDER BY UPDATED_AT DESC
        LIMIT 1
    """, (PIPELINE_NAME, QUERY))

    checkpoint = cursor.fetchone()


# ==========================================
# 6. RESTORE CHECKPOINT STATE
# ==========================================

(
    SAVED_PIPELINE,
    SAVED_QUERY,
    SAVED_START,
    SAVED_END,
    NEXT_TOKEN,
    PAGES_COMPLETED,
    TOTAL_POSTS_RETURNED,
    TOTAL_UNIQUE_POSTS,
    IS_COMPLETE
) = checkpoint


# Always reuse the original extraction window.
START_TIME = format_snowflake_timestamp(SAVED_START)
END_TIME = format_snowflake_timestamp(SAVED_END)


# ==========================================
# 7. DISPLAY CHECKPOINT DETAILS
# ==========================================

print("\nCHECKPOINT DETAILS")
print("-" * 55)

print("Pipeline:", SAVED_PIPELINE)
print("Query:", SAVED_QUERY)
print("Start:", START_TIME)
print("End:", END_TIME)
print("Pages completed:", PAGES_COMPLETED)
print("Posts returned:", TOTAL_POSTS_RETURNED)
print("Unique posts:", TOTAL_UNIQUE_POSTS)
print("Next token available:", bool(NEXT_TOKEN))
print("Extraction complete:", IS_COMPLETE)

if IS_COMPLETE:

    cursor.close()
    conn.close()

    raise SystemExit(
        "\nExtraction already completed. "
        "No new API requests will be made."
    )

print("\nCheckpoint initialization successful!")

# ==========================================
# 8. CREATE TEMPORARY STAGING TABLE
# ==========================================

STAGING_TABLE = "TMP_X_POSTS_STAGE"

print("\nPreparing Snowflake staging table...")

cursor.execute(f"""
    CREATE TEMPORARY TABLE IF NOT EXISTS {STAGING_TABLE} (
        POST_ID VARCHAR NOT NULL,
        CREATED_AT TIMESTAMP_TZ,
        AUTHOR_ID VARCHAR,
        RAW_PAYLOAD VARIANT,
        BATCH_ID VARCHAR,
        SOURCE_QUERY VARCHAR
    )
""")

conn.commit()

print("Staging table ready!")

# ==========================================
# 9. FETCH X API PAGE
# ==========================================

MAX_RETRIES = 5


def fetch_api_page(page_number, next_token=None):

    params = {
        "query": QUERY,
        "start_time": START_TIME,
        "end_time": END_TIME,
        "max_results": MAX_RESULTS,

        "tweet.fields": (
            "id,text,author_id,created_at,lang,"
            "public_metrics,entities,conversation_id,"
            "in_reply_to_user_id,referenced_tweets"
        ),

        "expansions": "author_id",

        "user.fields": (
            "id,name,username,verified,public_metrics"
        )
    }

    if next_token:
        params["next_token"] = next_token

    for attempt in range(1, MAX_RETRIES + 1):

        print("\n" + "=" * 55)
        print(f"Fetching page: {page_number}")
        print(f"Attempt: {attempt}/{MAX_RETRIES}")
        print("=" * 55)

        try:

            response = requests.get(
                URL,
                headers=HEADERS,
                params=params,
                timeout=60
            )

            print("HTTP Status:", response.status_code)

            # Rate limit handling
            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                reset_time = response.headers.get(
                    "x-rate-limit-reset"
                )

                if retry_after:
                    wait_time = int(retry_after)

                elif reset_time:
                    wait_time = max(
                        1,
                        int(reset_time) - int(time.time())
                    )

                else:
                    wait_time = min(
                        60 * attempt,
                        300
                    )

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                print(
                    f"Rate limit reached. "
                    f"Waiting {wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            # Retry temporary server failures
            if response.status_code in (500, 502, 503, 504):

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                wait_time = min(2 ** attempt, 60)

                print(
                    f"Temporary server error. "
                    f"Retrying in {wait_time} seconds..."
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

            result = response.json()

            if not isinstance(result, dict):
                raise ValueError(
                    "Unexpected X API response format"
                )

            if result.get("errors"):
                raise RuntimeError(
                    f"X API returned errors: {result['errors']}"
                )

            posts = result.get("data", [])

            if not isinstance(posts, list):
                raise ValueError(
                    "X API data field is not a list"
                )

            print("Posts retrieved:", len(posts))

            print(
                "Next page available:",
                bool(result.get("meta", {}).get("next_token"))
            )

            return result

        except requests.exceptions.RequestException as error:

            if attempt == MAX_RETRIES:
                raise

            wait_time = min(2 ** attempt, 60)

            print("Network/API error:", error)
            print(f"Retrying in {wait_time} seconds...")

            time.sleep(wait_time)

    raise RuntimeError(
        f"Unable to retrieve page {page_number}"
    )

# ==========================================
# 10. BULK LOAD POSTS INTO SNOWFLAKE
# ==========================================


def load_posts_to_snowflake(posts):

    if not posts:
        print("No posts to load.")
        return 0

    # ======================================
    # 1. VALIDATE AND PREPARE RECORDS
    # ======================================

    rows = []

    for post in posts:

        required = [
            "id",
            "created_at",
            "author_id"
        ]

        missing = [
            field for field in required
            if not post.get(field)
        ]

        if missing:
            raise ValueError(
                f"Post {post.get('id')} missing: {missing}"
            )

        payload = json.dumps(
            post,
            ensure_ascii=False
        )

        rows.append((
            str(post["id"]),
            post["created_at"],
            str(post["author_id"]),
            payload,
            BATCH_ID,
            QUERY
        ))

    # ======================================
    # 2. CLEAR STAGING TABLE
    # ======================================

    cursor.execute(
        f"DELETE FROM {STAGING_TABLE}"
    )

    # ======================================
    # 3. INSERT INTO STAGING TABLE
    # ======================================

    insert_sql = f"""
        INSERT INTO {STAGING_TABLE} (
            POST_ID,
            CREATED_AT,
            AUTHOR_ID,
            RAW_PAYLOAD,
            BATCH_ID,
            SOURCE_QUERY
        )
        SELECT
            %s,
            TO_TIMESTAMP_TZ(%s),
            %s,
            PARSE_JSON(%s),
            %s,
            %s
    """

    for row in rows:
        cursor.execute(insert_sql, row)

    print(
        "Records staged successfully:",
        len(rows)
    )

    # ======================================
    # 4. MERGE INTO BRONZE TABLE
    # ======================================

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS target

        USING (
            SELECT *
            FROM {STAGING_TABLE}

            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY POST_ID
                ORDER BY CREATED_AT DESC
            ) = 1

        ) AS source

        ON target.POST_ID = source.POST_ID

        WHEN NOT MATCHED THEN

            INSERT (
                POST_ID,
                CREATED_AT,
                AUTHOR_ID,
                RAW_PAYLOAD,
                BATCH_ID,
                SOURCE_QUERY
            )

            VALUES (
                source.POST_ID,
                source.CREATED_AT,
                source.AUTHOR_ID,
                source.RAW_PAYLOAD,
                source.BATCH_ID,
                source.SOURCE_QUERY
            )
    """

    cursor.execute(merge_sql)

    inserted = cursor.rowcount

    print(
        "MERGE completed. Rows inserted:",
        inserted
    )

    return inserted

# ==========================================
# 11. SAVE PAGE AND UPDATE CHECKPOINT
# ==========================================

def persist_page(
    page_number,
    api_response,
    posts,
    next_token
):

    try:

        print("\nPersisting page:", page_number)

        # 1. Load individual posts
        inserted = load_posts_to_snowflake(posts)

        # 2. Save complete raw API response
        cursor.execute(f"""
            INSERT INTO {AUDIT_TABLE} (
                BATCH_ID,
                PAGE_NUMBER,
                POST_COUNT,
                NEXT_TOKEN,
                RAW_RESPONSE
            )
            SELECT
                %s,
                %s,
                %s,
                %s,
                PARSE_JSON(%s)
        """, (
            BATCH_ID,
            page_number,
            len(posts),
            next_token,
            json.dumps(
                api_response,
                ensure_ascii=False
            )
        ))

        # 3. Calculate unique posts for this query
        cursor.execute(f"""
            SELECT COUNT(DISTINCT POST_ID)
            FROM {TARGET_TABLE}
            WHERE SOURCE_QUERY = %s
        """, (QUERY,))

        unique_posts = cursor.fetchone()[0]

        # 4. Update cumulative extraction state
        updated_posts = (
            TOTAL_POSTS_RETURNED + len(posts)
        )

        extraction_complete = (
            next_token is None
        )

        cursor.execute(f"""
            UPDATE {STATE_TABLE}

            SET
                NEXT_TOKEN = %s,

                PAGES_COMPLETED = %s,

                TOTAL_POSTS_RETURNED = %s,

                TOTAL_UNIQUE_POSTS = %s,

                IS_COMPLETE = %s,

                UPDATED_AT = CURRENT_TIMESTAMP()

            WHERE PIPELINE_NAME = %s
              AND QUERY_TEXT = %s
        """, (
            next_token,
            page_number,
            updated_posts,
            unique_posts,
            extraction_complete,
            PIPELINE_NAME,
            QUERY
        ))

        if cursor.rowcount != 1:
            raise RuntimeError(
                "Checkpoint update failed or matched "
                "an unexpected number of rows."
            )

        # 5. Commit everything together
        conn.commit()

        print("\nPAGE COMMITTED SUCCESSFULLY")

        print("Page number:", page_number)
        print("Posts returned:", len(posts))
        print("New posts inserted:", inserted)
        print("Total posts returned:", updated_posts)
        print("Unique posts in Snowflake:", unique_posts)
        print("Next page available:", bool(next_token))
        print("Extraction complete:", extraction_complete)

        return {
            "inserted": inserted,
            "total_returned": updated_posts,
            "unique_posts": unique_posts,
            "complete": extraction_complete
        }

    except Exception:

        conn.rollback()

        print(
            "\nPAGE TRANSACTION FAILED. "
            "Changes rolled back."
        )

        raise

# ==========================================
# 12. MAIN HISTORICAL EXTRACTION PIPELINE
# ==========================================

def run_historical_pipeline():

    global NEXT_TOKEN
    global PAGES_COMPLETED
    global TOTAL_POSTS_RETURNED
    global TOTAL_UNIQUE_POSTS

    print("\n")
    print("=" * 60)
    print("STARTING HISTORICAL EXTRACTION")
    print("=" * 60)

    pages_this_run = 0
    inserted_this_run = 0

    for _ in range(MAX_PAGES_PER_RUN):

        page_number = PAGES_COMPLETED + 1

        # Fetch next API page
        api_response = fetch_api_page(
            page_number,
            NEXT_TOKEN
        )

        posts = api_response.get("data", [])

        metadata = api_response.get("meta", {})

        new_token = metadata.get("next_token")

        # Persist page and checkpoint
        result = persist_page(
            page_number,
            api_response,
            posts,
            new_token
        )

        # Update local state only after commit
        PAGES_COMPLETED = page_number

        NEXT_TOKEN = new_token

        TOTAL_POSTS_RETURNED = result[
            "total_returned"
        ]

        TOTAL_UNIQUE_POSTS = result[
            "unique_posts"
        ]

        pages_this_run += 1

        inserted_this_run += result["inserted"]

        if result["complete"]:

            print(
                "\nNo more pages available. "
                "Historical extraction completed."
            )

            break

    # Final execution summary
    print("\n")
    print("=" * 60)
    print("HISTORICAL EXTRACTION RUN SUMMARY")
    print("=" * 60)

    print("Pipeline:", PIPELINE_NAME)
    print("Query:", QUERY)
    print("Batch ID:", BATCH_ID)

    print("Pages processed this run:", pages_this_run)

    print(
        "Total pages completed:",
        PAGES_COMPLETED
    )

    print(
        "New posts inserted this run:",
        inserted_this_run
    )

    print(
        "Total posts returned:",
        TOTAL_POSTS_RETURNED
    )

    print(
        "Total unique posts:",
        TOTAL_UNIQUE_POSTS
    )

    print(
        "Extraction complete:",
        NEXT_TOKEN is None
    )

    print("=" * 60)

# ==========================================
# 13. EXECUTE PIPELINE
# ==========================================

try:

    run_historical_pipeline()

except Exception as error:

    print("\nHISTORICAL PIPELINE FAILED")
    print("Error:", error)

    raise

finally:

    if cursor:
        cursor.close()

    if conn:
        conn.close()

    print("\nSnowflake connection closed.")