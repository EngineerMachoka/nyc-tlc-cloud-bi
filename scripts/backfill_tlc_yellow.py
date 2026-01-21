import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
import pyodbc

CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v


def is_current_month(y: int, m: int) -> bool:
    t = datetime.today()
    return t.year == y and t.month == m


def build_months(start_year: int, start_month: int, months_count: int):
    start = datetime(start_year, start_month, 1)
    out = []
    for i in range(months_count):
        d = start + relativedelta(months=i)
        out.append((d.year, d.month))
    return out


def months_last_36():
    start = datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=35)
    out = []
    for i in range(36):
        d = start + relativedelta(months=i)
        out.append((d.year, d.month))
    return out


def sql_connect():
    server = env("SQL_SERVER")
    db = env("SQL_DATABASE")
    user = env("SQL_USERNAME")
    pwd = env("SQL_PASSWORD")
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};Uid={user};Pwd={pwd};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)


def blob_container():
    bsc = BlobServiceClient(
        account_url=env("STORAGE_ACCOUNT_URL"),
        credential=env("STORAGE_ACCOUNT_KEY"),
    )
    return bsc.get_container_client(env("STORAGE_CONTAINER"))


def exists_in_download_log(cur, y: int, m: int) -> bool:
    cur.execute("""
        SELECT 1
        FROM dbo.download_log
        WHERE taxi_type='yellow' AND [year]=? AND [month]=?
    """, (y, m))
    return cur.fetchone() is not None


def exec_sql(cur, sql: str, params=()):
    cur.execute(sql, params)


def main():
    # Optional overrides (for manual runs / batching)
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
        file_parquet = f"yellow_tripdata_{y}-{m:02d}.parquet"
        url = f"{CLOUDFRONT_BASE}/{file_parquet}"

        # Blob path for CSV (what SQL BULK INSERT reads)
        csv_blob_path = f"csv/yellow/year={y}/yellow_tripdata_{y}-{m:02d}.csv"

        already_logged = exists_in_download_log(cur, y, m)

        # Skip historical already-processed months
        if already_logged and not is_current_month(y, m):
            skipped += 1
            continue

        try:
            # Current month refresh: delete SQL month first
            if already_logged and is_current_month(y, m):
                exec_sql(cur, "EXEC dbo.usp_delete_month_refresh @taxi_type=?, @year=?, @month=?",
                         ("yellow", y, m))
                cn.commit()

            # Log download start
            exec_sql(cur, """
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
                "yellow", y, m,
                f"yellow_tripdata_{y}-{m:02d}.csv",
                url,
                f"raw/{csv_blob_path}",
                "downloaded",
                "GitHub Actions: download parquet → convert CSV → BULK INSERT → stage → fact"
            ))
            cn.commit()

            # Download parquet
            r = requests.get(url, timeout=240)
            r.raise_for_status()

            # Convert parquet → CSV
            df = pd.read_parquet(BytesIO(r.content))
            csv_bytes = df.to_csv(index=False).encode("utf-8")

            # Upload CSV to Blob (overwrite safe for current month & re-runs)
            bc.upload_blob(name=csv_blob_path, data=csv_bytes, overwrite=True)

            # Load into SQL raw staging (server-side BULK INSERT via external data source)
            exec_sql(cur, "TRUNCATE TABLE dbo.stg_yellow_trip_raw;")
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
            exec_sql(cur, bulk_sql)
            cn.commit()

            # RAW → typed staging (you created this proc)
            exec_sql(cur, "EXEC dbo.usp_stage_from_raw_yellow @year=?, @month=?", (y, m))
            cn.commit()

            # typed staging → fact
            exec_sql(cur, "EXEC dbo.usp_load_month_from_staging @taxi_type=?, @year=?, @month=?",
                     ("yellow", y, m))
            cn.commit()

            processed += 1

        except Exception as e:
            failed += 1
            # log failure message into etl_run_log
            try:
                exec_sql(cur, """
                    INSERT INTO dbo.etl_run_log(taxi_type,[year],[month],status,message)
                    VALUES (?,?,?,'failed',?);
                """, ("yellow", y, m, str(e)[:3900]))
                cn.commit()
            except Exception:
                pass
            print(f"FAILED {y}-{m:02d}: {e}")

    cur.close()
    cn.close()

    print(f"Done. processed={processed}, skipped={skipped}, failed={failed}, total={len(months)}")
    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
