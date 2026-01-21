import os
import tempfile
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

TAXI_TYPE = "yellow"
EXTERNAL_DATA_SOURCE = "AzureBlobStorage"  # must match your SQL external data source name

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

def month_range(start_y: int, start_m: int, end_y: int, end_m: int):
    d = datetime(start_y, start_m, 1)
    end = datetime(end_y, end_m, 1)
    while d <= end:
        yield d.year, d.month
        d += relativedelta(months=1)

def is_current_month(y: int, m: int) -> bool:
    t = datetime.today()
    return t.year == y and t.month == m

def blob_container():
    svc = BlobServiceClient(account_url=env("STORAGE_ACCOUNT_URL"), credential=env("STORAGE_ACCOUNT_KEY"))
    return svc.get_container_client(env("STORAGE_CONTAINER"))  # should be 'raw'

def sql_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{env('SQL_SERVER')},1433;"
        f"Database={env('SQL_DATABASE')};"
        f"Uid={env('SQL_USERNAME')};"
        f"Pwd={env('SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)

def exists_in_download_log(cur, y: int, m: int) -> bool:
    cur.execute("""
        SELECT 1
        FROM dbo.download_log
        WHERE taxi_type=? AND [year]=? AND [month]=?
          AND status IN ('downloaded','loaded','completed')
    """, (TAXI_TYPE, y, m))
    return cur.fetchone() is not None

def main():
    # Range from env (workflow inputs), fallback defaults
    start_y = int(os.getenv("START_YEAR", "2024"))
    start_m = int(os.getenv("START_MONTH", "11"))
    end_y   = int(os.getenv("END_YEAR", "2024"))
    end_m   = int(os.getenv("END_MONTH", "11"))

    # Destructive switch for CSV blobs only
    delete_existing_csv = os.getenv("DELETE_EXISTING_CSV", "no").lower() == "yes"

    container = blob_container()
    cn = sql_conn()
    cur = cn.cursor()

    processed = skipped = failed = csv_deleted = 0

    print(f"DELETE_EXISTING_CSV = {delete_existing_csv}")
    print(f"Range: {start_y}-{start_m:02d} to {end_y}-{end_m:02d}")

    for (y, m) in month_range(start_y, start_m, end_y, end_m):
        parquet_blob = f"parquet/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.parquet"
        csv_blob     = f"csv/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.csv"

        print(f"\n=== {y}-{m:02d} ===")
        print(f"Parquet: {parquet_blob}")
        print(f"CSV:     {csv_blob}")

        try:
            already = exists_in_download_log(cur, y, m)
            if already and not is_current_month(y, m):
                print("SKIP → already processed (historical)")
                skipped += 1
                continue

            parquet_client = container.get_blob_client(parquet_blob)
            if not parquet_client.exists():
                print("SKIP → parquet not found in Blob")
                skipped += 1
                continue

            csv_client = container.get_blob_client(csv_blob)
            if csv_client.exists() and delete_existing_csv:
                csv_client.delete_blob()
                csv_deleted += 1
                print("Deleted existing CSV blob (DELETE_EXISTING_CSV=yes)")

            # Convert only if CSV not present (or deleted)
            if csv_client.exists() and not delete_existing_csv:
                print("CSV exists → reuse (no reconvert)")
            else:
                # Download parquet to temp file
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as pf:
                    parquet_path = pf.name
                    pf.write(parquet_client.download_blob().readall())

                # Convert to CSV temp file
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as cf:
                    csv_path = cf.name

                df = pd.read_parquet(parquet_path)
                df.to_csv(csv_path, index=False)

                # Upload CSV to Blob
                with open(csv_path, "rb") as f:
                    csv_client.upload_blob(f, overwrite=True)

                print("Converted and uploaded CSV")

            # Current month refresh (optional proc; if missing, it will fail here)
            if already and is_current_month(y, m):
                cur.execute("EXEC dbo.usp_delete_month_refresh @taxi_type=?, @year=?, @month=?",
                            (TAXI_TYPE, y, m))
                cn.commit()

            # Log (optional proc; if missing, ignore)
            try:
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
                    parquet_blob,
                    f"raw/{csv_blob}",
                    "downloaded",
                    "Blob parquet → Blob CSV → BULK INSERT → stage → fact"
                ))
                cn.commit()
            except Exception:
                cn.rollback()

            # Load CSV → SQL RAW (RAW is landing table, overwritten each month)
            cur.execute("TRUNCATE TABLE dbo.stg_yellow_trip_raw;")
            cn.commit()

            bulk_sql = f"""
            BULK INSERT dbo.stg_yellow_trip_raw
            FROM '{csv_blob}'
            WITH (
                DATA_SOURCE = '{EXTERNAL_DATA_SOURCE}',
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
            cur.execute("EXEC dbo.usp_stage_from_raw_yellow @year=?, @month=?", (y, m))
            cn.commit()

            # typed staging → fact
            cur.execute("EXEC dbo.usp_load_month_from_staging @taxi_type=?, @year=?, @month=?",
                        (TAXI_TYPE, y, m))
            cn.commit()

            print("SUCCESS → month loaded into FACT")
            processed += 1

        except Exception as e:
            print(f"FAILED → {e}")
            failed += 1
            try:
                cur.execute("""
                    INSERT INTO dbo.etl_run_log(taxi_type,[year],[month],status,message)
                    VALUES (?,?,?,'failed',?);
                """, (TAXI_TYPE, y, m, str(e)[:3900]))
                cn.commit()
            except Exception:
                pass

    cur.close()
    cn.close()
    print(f"\nDONE → processed={processed}, skipped={skipped}, failed={failed}, csv_deleted={csv_deleted}")

    if failed > 0:
        print("Completed with some failures. Check logs above.")


if __name__ == "__main__":
    main()
