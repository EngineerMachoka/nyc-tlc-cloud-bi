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
END_YEAR, END_MONTH     = 2025, 10

# Azure Blob (container only, no prefix)
STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
STORAGE_CONTAINER   = os.environ["STORAGE_CONTAINER"]  # should be 'raw'
STORAGE_ACCOUNT_KEY = os.environ["STORAGE_ACCOUNT_KEY"]

# SAFETY SWITCH (DEFAULT = NO DELETE)
DELETE_EXISTING = os.getenv("DELETE_EXISTING", "no").lower() == "yes"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "identity",
}

TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024
RETRIES = 3
MIN_BYTES = 1024


# =================================================
# HELPERS
# =================================================
def month_range(start_year, start_month, end_year, end_month):
    d = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    while d <= end:
        yield d.year, d.month
        d += relativedelta(months=1)


def build_url(year, month):
    return f"{BASE_URL}/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"


def container_client():
    svc = BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL,
        credential=STORAGE_ACCOUNT_KEY,
    )
    return svc.get_container_client(STORAGE_CONTAINER)


def upload_stream_to_blob(url: str, blob_path: str, container):
    blob = container.get_blob_client(blob_path)

    with requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT) as r:
        if r.status_code in (403, 404):
            return -r.status_code

        r.raise_for_status()

        bytes_uploaded = 0

        def stream():
            nonlocal bytes_uploaded
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    bytes_uploaded += len(chunk)
                    yield chunk

        blob.upload_blob(stream(), overwrite=True)
        return bytes_uploaded


# =================================================
# MAIN
# =================================================
def main():
    container = container_client()

    uploaded = skipped = failed = deleted = 0

    print(f"\nDELETE_EXISTING = {DELETE_EXISTING}\n")

    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        url = build_url(y, m)

        # ✅ CORRECT PATH (NO raw/raw)
        blob_path = f"parquet/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.parquet"

        print(f"=== {y}-{m:02d} ===")
        print(f"URL : {url}")
        print(f"BLOB: {blob_path}")

        blob = container.get_blob_client(blob_path)

        try:
            # Optional delete (ONLY if explicitly enabled)
            if blob.exists() and DELETE_EXISTING:
                blob.delete_blob()
                deleted += 1
                print("DELETED existing blob (DELETE_EXISTING=yes)")

            # Skip if exists and not deleting
            if blob.exists() and not DELETE_EXISTING:
                print("SKIP → already exists (safe mode)")
                skipped += 1
                continue

            for attempt in range(1, RETRIES + 1):
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
            print(f"FAILED → {e}")
            failed += 1
            time.sleep(3)

    print("\n==============================")
    print("FINISHED")
    print(f"Uploaded: {uploaded}")
    print(f"Deleted:  {deleted}")
    print(f"Skipped:  {skipped}")
    print(f"Failed:   {failed}")
    print("==============================")

if __name__ == "__main__":
    main()
