
import os
import snowflake.connector

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection configuration
config = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "authenticator": "snowflake",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "TWITTER_ANALYTICS",
    "schema": "BRONZE",
    "role": os.getenv("SNOWFLAKE_ROLE")
}

# Remove optional unset parameters
config = {
    key: value
    for key, value in config.items()
    if value
}

print("=" * 50)
print("SNOWFLAKE CONNECTION TEST")
print("=" * 50)

conn = None

try:

    conn = snowflake.connector.connect(**config)

    print("\nSUCCESS: Connected to Snowflake!")

    cursor = conn.cursor()

    # Verify connection identity
    cursor.execute("""
        SELECT
            CURRENT_ACCOUNT(),
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_WAREHOUSE(),
            CURRENT_DATABASE(),
            CURRENT_SCHEMA()
    """)

    result = cursor.fetchone()

    print("\nCONNECTION DETAILS")

    labels = [
        "Account",
        "User",
        "Role",
        "Warehouse",
        "Database",
        "Schema"
    ]

    for label, value in zip(labels, result):
        print(f"{label}: {value}")

    # Verify Bronze table
    print("\nVERIFYING BRONZE TABLES")

    cursor.execute("""
        SHOW TABLES IN SCHEMA TWITTER_ANALYTICS.BRONZE
    """)

    for table in cursor.fetchall():
        print("Table:", table[1])

    # Verify extraction control table
    print("\nVERIFYING CONTROL TABLE")

    cursor.execute("""
        SELECT COUNT(*)
        FROM TWITTER_ANALYTICS.CONTROL.EXTRACTION_STATE
    """)

    print("Extraction state records:", cursor.fetchone()[0])

    print("\nSNOWFLAKE CONNECTION TEST COMPLETED")

    cursor.close()

except Exception as error:

    print("\nSNOWFLAKE CONNECTION FAILED")
    print("Error:", error)

finally:

    if conn:
        conn.close()
        print("\nConnection closed.")