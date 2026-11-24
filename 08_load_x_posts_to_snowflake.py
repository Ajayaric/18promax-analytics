import os
import json
import uuid

from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

INPUT_FILE = BASE_DIR / "data" / "04_posts_pilot.json"

DATABASE = "TWITTER_ANALYTICS"
SCHEMA = "BRONZE"

TARGET_TABLE = f"{DATABASE}.{SCHEMA}.RAW_X_POSTS"

SOURCE_QUERY = "#18promax"

BATCH_ID = str(uuid.uuid4())


# ============================================================
# 2. SNOWFLAKE CONNECTION
# ============================================================

config = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "authenticator": "snowflake",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": DATABASE,
    "schema": SCHEMA,
    "role": os.getenv("SNOWFLAKE_ROLE")
}

config = {
    key: value
    for key, value in config.items()
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
    if field not in config
]

if missing_fields:
    raise ValueError(
        f"Missing Snowflake configuration: {missing_fields}"
    )


# ============================================================
# 3. READ SOURCE JSON
# ============================================================

def read_posts():

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:

        response = json.load(file)

    posts = response.get("data", [])

    if not isinstance(posts, list):
        raise ValueError(
            "Expected 'data' to contain a list of posts"
        )

    # Validate mandatory fields
    for post in posts:

        if not post.get("id"):
            raise ValueError("Post missing ID")

        if not post.get("created_at"):
            raise ValueError(
                f"Post {post.get('id')} missing created_at"
            )

        if not post.get("author_id"):
            raise ValueError(
                f"Post {post.get('id')} missing author_id"
            )

    return posts


# ============================================================
# 4. SNOWFLAKE MERGE
# ============================================================

def load_posts(cursor, posts):

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS target

        USING (
            SELECT
                %s::VARCHAR AS POST_ID,
                TO_TIMESTAMP_TZ(%s) AS CREATED_AT,
                %s::VARCHAR AS AUTHOR_ID,
                PARSE_JSON(%s) AS RAW_PAYLOAD,
                %s::VARCHAR AS BATCH_ID,
                %s::VARCHAR AS SOURCE_QUERY
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

    for post in posts:

        payload = json.dumps(
            post,
            ensure_ascii=False
        )

        cursor.execute(
            merge_sql,
            (
                str(post["id"]),
                post["created_at"],
                str(post["author_id"]),
                payload,
                BATCH_ID,
                SOURCE_QUERY
            )
        )


# ============================================================
# 5. VALIDATE SNOWFLAKE LOAD
# ============================================================

def validate_load(cursor):

    cursor.execute(f"""
        SELECT
            COUNT(*) AS TOTAL_RECORDS,
            COUNT(DISTINCT POST_ID) AS UNIQUE_POSTS
        FROM {TARGET_TABLE}
    """)

    total_records, unique_posts = cursor.fetchone()

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {TARGET_TABLE}
        WHERE BATCH_ID = %s
    """, (BATCH_ID,))

    batch_records = cursor.fetchone()[0]

    print("\nSNOWFLAKE VALIDATION")
    print("-" * 50)

    print("Total records in Bronze:", total_records)
    print("Unique post IDs:", unique_posts)
    print("Records inserted in this batch:", batch_records)

    return total_records, unique_posts, batch_records


# ============================================================
# 6. MAIN EXECUTION
# ============================================================

def main():
    print("=" * 55)
    print("X HISTORICAL POSTS - SNOWFLAKE INGESTION")
    print("=" * 55)

    posts = read_posts()

    print("\nSOURCE DATA")
    print("-" * 50)
    print("Input file:", INPUT_FILE.name)
    print("Posts available:", len(posts))
    print("Source query:", SOURCE_QUERY)
    print("Batch ID:", BATCH_ID)

    conn = None
    cursor = None

    try:

        print("\nConnecting to Snowflake...")

        conn = snowflake.connector.connect(**config)

        conn.autocommit(False)

        cursor = conn.cursor()

        print("Snowflake connection successful!")

        # Load records
        print("\nLoading posts into Bronze...")

        load_posts(cursor, posts)

        # Validate before committing
        total, unique, batch = validate_load(cursor)

        if total != unique:
            raise ValueError(
                "Duplicate POST_ID values detected!"
            )

        conn.commit()

        print("\nTransaction committed successfully!")

        print("\n" + "=" * 55)
        print("INGESTION COMPLETED SUCCESSFULLY")
        print("=" * 55)

        print("Source posts:", len(posts))
        print("New records inserted:", batch)
        print("Total Bronze records:", total)
        print("Unique posts:", unique)

    except Exception as error:

        if conn:
            conn.rollback()

        print("\nINGESTION FAILED")
        print("Error:", error)

        raise

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()

            print("\nSnowflake connection closed.")


if __name__ == "__main__":
    main()