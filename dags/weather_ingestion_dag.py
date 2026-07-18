import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from Weather_API_Connector import main


@dag(
    dag_id="weather_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2026,7,1, tz="UTC"),
    catchup=False,
    tags=["weather","learning"],
)

def weather_ingestion_dag():


    @task
    def run_weather_pipeline():
        main()

    run_weather_pipeline()

weather_ingestion_dag()

