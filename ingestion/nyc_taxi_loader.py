import os
import io
import requests
import pandas as pd
from sqlalchemy import create_engine, text

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SOURCE_NAME = "nyc_taxi_yellow"

POSTGRES_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN") 


def month_to_filename(month: str) -> str:
   
    return f"yellow_tripdata_{month}.parquet"

def download_parquet(month: str) -> bytes:
    url = f"{BASE_URL}/{month_to_filename(month)}"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
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
   
    for k, v in list(rename_map.items()):
        if k in df.columns:
            df = df.rename(columns={k: v})
    
    expected = [
        "vendor_id","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
        "ratecode_id","store_and_fwd_flag","pu_location_id","do_location_id","payment_type",
        "fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge",
        "total_amount","congestion_surcharge","airport_fee"
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None
    return df[expected]

def get_engine():
    if not POSTGRES_CONN:
        raise RuntimeError("Missing AIRFLOW__DATABASE__SQL_ALCHEMY_CONN env var inside container.")
    return create_engine(POSTGRES_CONN)

def ensure_watermark(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.ingestion_watermark (
                source_name TEXT PRIMARY KEY,
                last_loaded_month DATE NOT NULL
            )
        """))

def get_last_loaded_month(engine):
    ensure_watermark(engine)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT last_loaded_month FROM raw.ingestion_watermark WHERE source_name = :s"),
            {"s": SOURCE_NAME},
        ).fetchone()
        return row[0] if row else None

def set_last_loaded_month(engine, month_date):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_watermark (source_name, last_loaded_month)
                VALUES (:s, :m)
                ON CONFLICT (source_name) DO UPDATE SET last_loaded_month = EXCLUDED.last_loaded_month
            """),
            {"s": SOURCE_NAME, "m": month_date},
        )

def load_month(month: str):
    engine = get_engine()

    parquet_bytes = download_parquet(month)
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    df = normalize_columns(df)
    df["source_file"] = month_to_filename(month)

    
    df.to_sql("nyc_taxi_yellow_trips", engine, schema="raw", if_exists="append", index=False, chunksize=50_000, method="multi")

    
    month_date = pd.to_datetime(month + "-01").date()
    set_last_loaded_month(engine, month_date)

    return {"month": month, "rows_loaded": len(df)}

if __name__ == "__main__":
  
    print(load_month("2024-01"))
