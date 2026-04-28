from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from anidata_scraper.scraper import scrape_to_file

DEFAULT_ARGS = {
    "owner": "anidata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="scraper_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["scraping"],
) as dag:

    scrape = PythonOperator(
        task_id="scrape_anime_site",
        python_callable=scrape_to_file,
        op_kwargs={
            "output_dir": "/opt/airflow/data/raw",
            "base_url": "http://mock-site",
            "enrich": True,
        },
    )

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_dag",
        trigger_dag_id="etl_dag",
        wait_for_completion=False,
    )

    scrape >> trigger_etl

    # test pipeline complet
    