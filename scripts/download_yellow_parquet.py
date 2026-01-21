import requests
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient

# =================================================
# CONFIG
# =================================================
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow"

START_YEAR, START_MONTH = 2022, 10
END_YEAR, END_MONTH = 2025, 10

# Azure Blob
STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
STORAGE_CONTAINER = os.environ["STORAGE_CONTAINER"]
STORAGE_ACCOUNT_KEY = os.environ["STORAGE_ACCOUNT_KEY"]

# Request config (browser-like)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
}

TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024  # 1MB
RETRIES = 3
MIN_BYTES = 1024


# =================================================
# HELPERS
# =================================================
def month_range(start_year, start_month, end_year, end_month):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    d = start
    while d <= end:
        yield d.year, d.month
        d += relativedelta(months=1)


def build_url(year, month):
    return f"{BASE_URL}/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"


def blob_client():
    service = BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL,
        credential=STORAGE_ACCOUNT_KEY,
    )
    return service.get_container_client(STORAGE_CONTAINER)


def upload_stream_to_blob(url: str, blob_path: str, container):
    """
    Stream-download a URL and stream-upload directly to Azure Blob.
    Returns number of bytes uploaded.
    """
    blob = container.get_blob_client(blob_path)

    with requests.get(
        url,
        headers=HEADERS,
        stream=True,
        timeout=TIMEOUT,
        allow_redirects=True,
    ) as r:

        if r.status_code in (403, 404):
            return -r.status_code

        r.raise_for_status()

        bytes_uploaded = 0

        def gen():
            nonlocal bytes_uploaded
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    bytes_uploaded += len(chunk)
                    yield chunk

        blob.upload_blob(gen(), overwrite=True)
        return bytes_uploaded


# =================================================
# MAIN
# =================================================
def main():
    container = blob_client()

    uploaded = skipped = failed = 0

    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        url = build_url(y, m)
        blob_path = f"parquet/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.parquet"

        print(f"\n=== {y}-{m:02d} ===")
        print(f"URL:  {url}")
        print(f"BLOB: {blob_path}")

        for attempt in range(1, RETRIES + 1):
            try:
                # Skip if blob already exists (historical months)
                blob = container.get_blob_client(blob_path)
                if blob.exists():
                    print("SKIP → already exists in Blob")
                    skipped += 1
                    break

                result = upload_stream_to_blob(url, blob_path, container)

                if result in (-403, -404):
                    print(f"SKIP → HTTP {abs(result)}")
                    skipped += 1
                    break

                if result < MIN_BYTES:
                    raise RuntimeError(f"Invalid upload size ({result} bytes)")

                print(f"UPLOADED → {result} bytes")
                uploaded += 1
                break

            except Exception as e:
                print(f"Attempt {attempt}/{RETRIES} failed: {e}")
                if attempt < RETRIES:
                    time.sleep(3 * attempt)
                else:
                    print("FAILED → giving up")
                    failed += 1

    print("\n==============================")
    print("FINISHED")
    print(f"Uploaded:  {uploaded}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")
    print("==============================")


if __name__ == "__main__":
    main()
