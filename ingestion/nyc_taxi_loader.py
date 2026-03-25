import io
import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SOURCE_NAME = "nyc_taxi_yellow"

POSTGRES_CONN = os.getenv("DW_CONN")


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────


def month_to_filename(month: str) -> str:
    return f"yellow_tripdata_{month}.parquet"


def get_engine():
    if not POSTGRES_CONN:
        raise RuntimeError("Missing DW_CONN env var. Check docker-compose.yml.")
    return create_engine(
        POSTGRES_CONN,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )


# ──────────────────────────────────────────
# Watermark
# ──────────────────────────────────────────


def ensure_watermark(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS raw.ingestion_watermark (
                source_name TEXT PRIMARY KEY,
                last_loaded_month DATE NOT NULL
            )
        """)
        )


def get_last_loaded_month(engine) -> Any:
    ensure_watermark(engine)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT last_loaded_month FROM raw.ingestion_watermark WHERE source_name = :s"
            ),
            {"s": SOURCE_NAME},
        ).fetchone()
        return row[0] if row else None


def set_last_loaded_month(engine, month_date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_watermark (source_name, last_loaded_month)
                VALUES (:s, :m)
                ON CONFLICT (source_name) DO UPDATE SET last_loaded_month = EXCLUDED.last_loaded_month
            """),
            {"s": SOURCE_NAME, "m": month_date},
        )


# ──────────────────────────────────────────
# Audit
# ──────────────────────────────────────────


def write_audit(
    engine,
    *,
    month: str,
    source_file: str,
    status: str,
    rows_loaded: int | None = None,
    error_message: str | None = None,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    duration = (finished_at - started_at).total_seconds()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_audit
                    (source_name, month, source_file, status, rows_loaded,
                     error_message, started_at, finished_at, duration_secs)
                VALUES
                    (:src, :month, :file, :status, :rows,
                     :err, :started, :finished, :dur)
            """),
            {
                "src": SOURCE_NAME,
                "month": month,
                "file": source_file,
                "status": status,
                "rows": rows_loaded,
                "err": error_message,
                "started": started_at,
                "finished": finished_at,
                "dur": duration,
            },
        )
    log.info(
        "[audit] month=%s status=%s rows=%s duration=%.1fs",
        month,
        status,
        rows_loaded,
        duration,
    )


# ──────────────────────────────────────────
# Download + normalize
# ──────────────────────────────────────────


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def download_parquet(month: str) -> bytes:
    url = f"{BASE_URL}/{month_to_filename(month)}"
    log.info("[download] Fetching %s", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    log.info("[download] %.1f MB received", len(r.content) / 1_000_000)
    return r.content


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "ratecode_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    expected = [
        "vendor_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None
    return df[expected]


# ──────────────────────────────────────────
# Main load function
# ──────────────────────────────────────────


def load_month(month: str) -> dict[str, Any]:
    engine = get_engine()
    source_file = month_to_filename(month)
    started_at = datetime.now(timezone.utc)

    log.info("=" * 60)
    log.info("[pipeline] START  month=%s  file=%s", month, source_file)

    try:
        # Download
        parquet_bytes = download_parquet(month)
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        log.info("[load] Rows in parquet: %d", len(df))

        # Normalize
        df = normalize_columns(df)
        df["source_file"] = source_file

        # Delete existing rows for this month (idempotent)
        with engine.begin() as conn:
            deleted = conn.execute(
                text("DELETE FROM raw.nyc_taxi_yellow_trips WHERE source_file = :f"),
                {"f": source_file},
            ).rowcount
        if deleted:
            log.info("[load] Deleted %d existing rows for %s", deleted, source_file)

        # Insert
        log.info("[load] Inserting %d rows into raw.nyc_taxi_yellow_trips...", len(df))
        df.to_sql(
            "nyc_taxi_yellow_trips",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=50_000,
            method="multi",
        )

        # Update watermark
        month_date = pd.to_datetime(month + "-01").date()
        set_last_loaded_month(engine, month_date)

        finished_at = datetime.now(timezone.utc)
        write_audit(
            engine,
            month=month,
            source_file=source_file,
            status="success",
            rows_loaded=len(df),
            started_at=started_at,
            finished_at=finished_at,
        )

        log.info("[pipeline] END  month=%s  rows=%d", month, len(df))
        log.info("=" * 60)
        return {"month": month, "rows_loaded": len(df)}

    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        log.error("[pipeline] FAILED  month=%s  error=%s", month, str(e))
        write_audit(
            engine,
            month=month,
            source_file=source_file,
            status="error",
            rows_loaded=None,
            error_message=str(e),
            started_at=started_at,
            finished_at=finished_at,
        )
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(load_month("2024-01"))
