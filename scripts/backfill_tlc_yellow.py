import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
import pyodbc

FORCE_REPROCESS = os.getenv("FORCE_REPROCESS", "false").lower() == "true"

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow"


# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing environment variable: {name}")
    return val


def is_current_month(y: int, m: int) -> bool:
    today = datetime.today()
    return today.year == y and today.month == m


def months_last_36():
    start = datetime.today().replace(day=1) - relativedelta(months=35)
    months = []
    for i in range(36):
        d = start + relativedelta(months=i)
        months.append((d.year, d.month))
    return months


def build_months(start_year: int, start_month: int, count: int):
    start = datetime(start_year, start_month, 1)
    months = []
    for i in range(count):
        d = start + relativedelta(months=i)
        months.append((d.year, d.month))
    return months


def sql_connect():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{env('SQL_SERVER')},1433;"
        f"Database={env('SQL_DATABASE')};"
        f"Uid={env('SQL_USERNAME')};"
        f"Pwd={env('SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)


def blob_container():
    service = BlobServiceClient(
        account_url=env("STORAGE_ACCOUNT_URL"),
        credential=env("STORAGE_ACCOUNT_KEY"),
    )
    return service.get_container_client(env("STORAGE_CONTAINER"))


def exists_in_download_log(cur, y: int, m: int) -> bool:
    cur.execute("""
        SELECT 1
        FROM dbo.download_log
        WHERE taxi_type = ?
          AND [year] = ?
          AND [month] = ?
          AND status IN ('downloaded','loaded','completed')
    """, (TAXI_TYPE, y, m))
    return cur.fetchone() is not None


def log_etl(cur, y, m, status, message):
    cur.execute("""
        INSERT INTO dbo.etl_run_log (taxi_type,[year],[month],status,message)
        VALUES (?,?,?,?,?)
    """, (TAXI_TYPE, y, m, status, message[:3900]))


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    # Optional overrides (used by GitHub Actions manual runs)
    start_year = os.getenv("START_YEAR")
    start_month = os.getenv("START_MONTH")
    months_count = os.getenv("MONTHS_COUNT")

    if start_year and start_month and months_count:
        months = build_months(int(start_year), int(start_month), int(months_count))
    else:
        months = months_last_36()

    bc = blob_container()
    cn = sql_connect()
    cur = cn.cursor()

    processed = 0
    skipped = 0
    failed = 0

    for (y, m) in months:
        print(f"\n=== Processing {y}-{m:02d} ===")

        parquet_name = f"yellow_tripdata_{y}-{m:02d}.parquet"
        parquet_url = f"{CLOUDFRONT_BASE}/{parquet_name}"
        csv_blob_path = f"csv/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.csv"

        # Skip historical months already processed
        if exists_in_download_log(cur, y, m) and not is_current_month(y, m) and not FORCE_REPROCESS:
            skipped += 1
            continue


        try:
            # Refresh current month if needed
            if exists_in_download_log(cur, y, m) and is_current_month(y, m):
                cur.execute(
                    "EXEC dbo.usp_delete_month_refresh @taxi_type=?, @year=?, @month=?",
                    (TAXI_TYPE, y, m),
                )
                cn.commit()

            # Download parquet
            r = requests.get(parquet_url, timeout=240)

            # 404 or missing file → SKIP (do not fail)
            if r.status_code == 404:
                print("SKIP: parquet not available (404)")
                log_etl(cur, y, m, "skipped", "Parquet not available at source (404)")
                cn.commit()
                skipped += 1
                continue

            r.raise_for_status()

            # Empty / invalid response
            if not r.content or len(r.content) < 1024:
                print("SKIP: empty or invalid parquet response")
                log_etl(cur, y, m, "skipped", "Empty or invalid parquet response")
                cn.commit()
                skipped += 1
                continue

            # Convert parquet → CSV
            try:
                df = pd.read_parquet(BytesIO(r.content))
            except Exception as e:
                print(f"SKIP: parquet parse error: {e}")
                log_etl(cur, y, m, "skipped", f"Parquet parse error: {e}")
                cn.commit()
                skipped += 1
                continue

            csv_bytes = df.to_csv(index=False).encode("utf-8")

            # Upload CSV to Blob
            bc.upload_blob(name=csv_blob_path, data=csv_bytes, overwrite=True)

            # Log download
            cur.execute("""
                EXEC dbo.usp_log_download_start
                    @taxi_type=?,
                    @year=?,
                    @month=?,
                    @file_name=?,
                    @file_url=?,
                    @blob_path=?,
                    @status=?,
                    @message=?;
            """, (
                TAXI_TYPE, y, m,
                f"yellow_tripdata_{y}-{m:02d}.csv",
                parquet_url,
                f"raw/{csv_blob_path}",
                "downloaded",
                "GitHub Actions: parquet → CSV → Blob → SQL"
            ))
            cn.commit()

            # Load into SQL RAW via BULK INSERT
            cur.execute("TRUNCATE TABLE dbo.stg_yellow_trip_raw;")
            cn.commit()

            bulk_sql = f"""
            BULK INSERT dbo.stg_yellow_trip_raw
            FROM '{csv_blob_path}'
            WITH (
                DATA_SOURCE = 'AzureBlobStorage',
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '0x0a',
                TABLOCK,
                CODEPAGE = '65001'
            );
            """
            cur.execute(bulk_sql)
            cn.commit()

            # RAW → typed staging
            cur.execute(
                "EXEC dbo.usp_stage_from_raw_yellow @year=?, @month=?",
                (y, m),
            )
            cn.commit()

            # typed staging → fact
            cur.execute(
                "EXEC dbo.usp_load_month_from_staging @taxi_type=?, @year=?, @month=?",
                (TAXI_TYPE, y, m),
            )
            cn.commit()

            processed += 1
            print("SUCCESS")

        except Exception as e:
            failed += 1
            print(f"FAILED: {e}")
            try:
                log_etl(cur, y, m, "failed", str(e))
                cn.commit()
            except Exception:
                pass

    cur.close()
    cn.close()

    print(
        f"\nDONE → processed={processed}, skipped={skipped}, failed={failed}, total={len(months)}"
    )

    # Do NOT fail workflow just because some months were skipped
    if failed > 0:
        print("Completed with some failures. Check etl_run_log.")


if __name__ == "__main__":
    main()
