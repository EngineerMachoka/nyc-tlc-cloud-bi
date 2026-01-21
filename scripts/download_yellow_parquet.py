import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

# ---------------------------------------
# CONFIG
# ---------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow"

START_YEAR = 2022
START_MONTH = 10

END_YEAR = 2025
END_MONTH = 10

DOWNLOAD_DIR = Path("downloads/yellow_parquet")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT_SECONDS = 240
MIN_VALID_BYTES = 1024  # protects against empty responses

# ---------------------------------------
# HELPERS
# ---------------------------------------
def month_range(start_year, start_month, end_year, end_month):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)

    d = start
    while d <= end:
        yield d.year, d.month
        d += relativedelta(months=1)


def build_url(year, month):
    return f"{BASE_URL}/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"


def download_parquet(year, month):
    url = build_url(year, month)
    file_name = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"
    file_path = DOWNLOAD_DIR / file_name

    print(f"\n=== {year}-{month:02d} ===")
    print(f"URL: {url}")

    try:
        # HEAD check first (fast, avoids big downloads if missing)
        head = requests.head(url, timeout=30)

        if head.status_code == 404:
            print("SKIP → File not found (404)")
            return "skipped"

        # GET actual file
        response = requests.get(url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()

        if not response.content or len(response.content) < MIN_VALID_BYTES:
            print(f"SKIP → Invalid/empty file ({len(response.content)} bytes)")
            return "skipped"

        # Save file
        with open(file_path, "wb") as f:
            f.write(response.content)

        print(f"DOWNLOADED → {file_path}")
        return "downloaded"

    except Exception as e:
        print(f"FAILED → {e}")
        return "failed"


# ---------------------------------------
# MAIN
# ---------------------------------------
def main():
    downloaded = 0
    skipped = 0
    failed = 0

    for year, month in month_range(
        START_YEAR, START_MONTH, END_YEAR, END_MONTH
    ):
        result = download_parquet(year, month)

        if result == "downloaded":
            downloaded += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1

    print("\n==============================")
    print("FINISHED")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")
    print("==============================")

if __name__ == "__main__":
    main()
