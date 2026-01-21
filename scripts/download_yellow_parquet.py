import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import time

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow"

START_YEAR, START_MONTH = 2022, 10
END_YEAR, END_MONTH = 2025, 10

DOWNLOAD_DIR = Path("downloads/yellow_parquet")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Browser-like headers (very important)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "identity",  # avoid weird gzip/empty edge cases
    "Connection": "keep-alive",
}

TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024  # 1MB
RETRIES = 3


def month_range(start_year, start_month, end_year, end_month):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    d = start
    while d <= end:
        yield d.year, d.month
        d += relativedelta(months=1)


def build_url(year, month):
    return f"{BASE_URL}/{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"


def download_stream(url: str, out_path: Path) -> int:
    """
    Streams URL to file and returns number of bytes written.
    """
    with requests.get(url, headers=HEADERS, stream=True, timeout=TIMEOUT, allow_redirects=True) as r:
        # If file truly missing, CloudFront should return 403/404 sometimes
        if r.status_code in (403, 404):
            return -r.status_code

        r.raise_for_status()

        bytes_written = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:  # ignore keep-alive chunks
                    f.write(chunk)
                    bytes_written += len(chunk)

        return bytes_written


def safe_download_month(year, month):
    url = build_url(year, month)
    file_name = f"{TAXI_TYPE}_tripdata_{year}-{month:02d}.parquet"
    out_path = DOWNLOAD_DIR / file_name

    print(f"\n=== {year}-{month:02d} ===")
    print(f"URL: {url}")

    # Retry loop
    for attempt in range(1, RETRIES + 1):
        try:
            # If file already exists locally and has size > 0, skip
            if out_path.exists() and out_path.stat().st_size > 0:
                print(f"SKIP → already downloaded locally ({out_path.stat().st_size} bytes)")
                return "skipped_local"

            # Download
            result = download_stream(url, out_path)

            # Handle missing/blocked
            if result in (-403, -404):
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                print(f"SKIP → HTTP {abs(result)} (not accessible or not found)")
                return f"skipped_http{abs(result)}"

            # Validate
            if result <= 0:
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                raise RuntimeError(f"Downloaded 0 bytes (attempt {attempt})")

            print(f"DOWNLOADED → {out_path} ({result} bytes)")
            return "downloaded"

        except Exception as e:
            # Clean up partial file
            if out_path.exists() and out_path.stat().st_size == 0:
                out_path.unlink(missing_ok=True)

            print(f"Attempt {attempt}/{RETRIES} failed: {e}")
            if attempt < RETRIES:
                time.sleep(3 * attempt)  # simple backoff
                continue
            else:
                print("FAILED → giving up after retries")
                return "failed"


def main():
    downloaded = skipped = failed = 0

    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        status = safe_download_month(y, m)
        if status == "downloaded":
            downloaded += 1
        elif status.startswith("skipped"):
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
