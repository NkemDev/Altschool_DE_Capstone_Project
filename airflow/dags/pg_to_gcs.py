from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    "load_postgres_to_bigquery",
    default_args=default_args,
    description="An airflow DAG to load data from Postgres to Big Query",
    schedule_interval=None,  # Adjust as needed
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    table_list = [
        "olist_customers",
        "olist_geolocation",
        "olist_order_items",
        "olist_order_payments",
        "olist_order_reviews",
        "olist_orders",
        "olist_products",
        "olist_sellers",
        "product_category_name_translation",
    ]

    for table in table_list:
        postgres_to_gcs = PostgresToGCSOperator(
            task_id=f"postgres_to_gcs_{table}",
            postgres_conn_id="postgres_conn",
            sql=f"SELECT * FROM {table}",
            bucket="nkem_capstone_bucket",
            filename=f"{table}.json",
            export_format="json",
        )

        gcs_to_bigquery = GCSToBigQueryOperator(
            task_id=f"gcs_to_bigquery_{table}",
            bucket="nkem_capstone_bucket",
            source_objects=[f"{table}.json"],
            destination_project_dataset_table=f"luminous-pier-424818-g0.olist_datasets.{table}",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        start >> postgres_to_gcs >> gcs_to_bigquery >> end
