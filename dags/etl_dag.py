from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "anidata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_latest_to_elasticsearch(raw_dir: str, index: str) -> None:
    from elasticsearch import Elasticsearch

    raw_path = Path(raw_dir)
    files = sorted(raw_path.glob("anime_*.json"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier trouvé dans {raw_dir}")

    latest = files[-1]
    logger.info("Chargement de %s", latest)

    data = json.loads(latest.read_text(encoding="utf-8"))
    animes = data.get("animes", [])

    es = Elasticsearch("http://elasticsearch:9200")
    actions = [
        {"_index": index, "_id": a["id"], "_source": a}
        for a in animes
    ]

    from elasticsearch.helpers import bulk
    bulk(es, actions)
    logger.info("%d animes indexés dans '%s'", len(actions), index)


with DAG(
    dag_id="etl_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl"],
) as dag:

    load = PythonOperator(
        task_id="load_to_elasticsearch",
        python_callable=load_latest_to_elasticsearch,
        op_kwargs={
            "raw_dir": "/opt/airflow/data/raw",
            "index": "animes",
        },
    )