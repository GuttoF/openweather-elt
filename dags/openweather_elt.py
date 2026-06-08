from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

INCLUDE = Path("/usr/local/airflow/include")
DBT_PROJECT = INCLUDE / "dbt" / "openweather"

sys.path.insert(0, str(INCLUDE / "pipelines"))


@dag(
    dag_id="openweather_elt",
    schedule="@hourly",
    start_date=datetime(2026, 6, 8),
    catchup=False,
    tags=["openweather", "elt", "dlt", "dbt"],
)
def openweather_elt():
    @task
    def extract_load():
        import openweather

        openweather.run()

    transform = BashOperator(
        task_id="transform",
        bash_command=(f"cd {DBT_PROJECT} && dbt build --profiles-dir {DBT_PROJECT}"),
    )

    extract_load() >> transform


openweather_elt()
