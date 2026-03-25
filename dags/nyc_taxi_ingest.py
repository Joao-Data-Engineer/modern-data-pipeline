from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ingestion.nyc_taxi_loader import get_engine, get_last_loaded_month, load_month

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def compute_next_month_to_load(**context) -> str | None:
    engine = get_engine()
    last = get_last_loaded_month(engine)
    if last is None:
        next_month = "2024-01"
    else:
        y = last.year
        m = last.month
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        next_month = f"{y:04d}-{m:02d}"

    now = datetime.now(timezone.utc)
    if (int(next_month[:4]) > now.year) or (
        int(next_month[:4]) == now.year and int(next_month[5:7]) > now.month
    ):
        return None

    return next_month


def load_next_month(**context) -> dict:
    ti = context["ti"]
    month = ti.xcom_pull(task_ids="compute_next_month")
    if not month:
        return {"skipped": True, "reason": "No new month to load"}
    return load_month(month)


with DAG(
    dag_id="nyc_taxi_yellow_incremental",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["nyc-taxi", "incremental"],
    template_searchpath=["/opt/airflow", "/opt/airflow/dags", "/opt/airflow/warehouse"],
) as dag:
    create_schema_and_tables = PostgresOperator(
        task_id="create_schema_and_tables",
        postgres_conn_id="postgres_dw",
        sql="warehouse/schema.sql",
    )

    compute_next_month = PythonOperator(
        task_id="compute_next_month",
        python_callable=compute_next_month_to_load,
    )

    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=load_next_month,
    )

    create_schema_and_tables >> compute_next_month >> load_incremental
