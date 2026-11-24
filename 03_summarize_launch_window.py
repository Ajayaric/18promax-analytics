
import json
from pathlib import Path
from datetime import datetime, timezone

PROJECT_DIR = Path(__file__).resolve().parent
DATA_FILE = PROJECT_DIR / "data" / "02_historical_counts.jsonl"

START_DATE = datetime(2026, 9, 9, tzinfo=timezone.utc)
END_DATE = datetime(2026, 11, 10, tzinfo=timezone.utc)

daily_counts = {}

with open(DATA_FILE, "r", encoding="utf-8") as file:

    for line in file:

        if not line.strip():
            continue

        record = json.loads(line)

        bucket_start = datetime.fromisoformat(
            record["start"].replace("Z", "+00:00")
        )

        if START_DATE <= bucket_start < END_DATE:

            count = record.get(
                "tweet_count",
                record.get("post_count", 0)
            )

            daily_counts[record["start"]] = int(count)

total_posts = sum(daily_counts.values())

print("=" * 50)
print("18PROMAX LAUNCH PERIOD ANALYSIS")
print("=" * 50)

print("Hashtag: #18promax")
print("Start date:", START_DATE)
print("End date:", END_DATE)
print("Daily records:", len(daily_counts))
print("Total matching posts:", total_posts)

print("\nEstimated post-read cost at $0.005/post:")
print(f"${total_posts * 0.005:.2f}")

print("\nDaily breakdown:")

for date, count in sorted(daily_counts.items()):
    print(date, count)

print("\nAnalysis completed.")