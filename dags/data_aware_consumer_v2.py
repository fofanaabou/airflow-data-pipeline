from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.python import PythonOperator

events_dataset_09c = Dataset("/data/09_data_aware/events/09c")
events_dataset_09d = Dataset("/data/09_data_aware/events/09d")

def _calculate_states(input_path, output_path):
    """Calculates event statistics."""
    events = pd.read_json(input_path, convert_dates=['timestamp'], lines=True)

    stats = (
        events
        .assign(datetime=lambda df: df['timestamp'].dt.date())
        .groupby(['date', "user"]).size().reset_index()
    )

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


with DAG(
        dag_id="data_aware_consumer_v1",
        schedule=[events_dataset_09c, events_dataset_09d],
        start_date=pendulum.datetime(year=2025, month=9, day=10),
):
    calculate_stats = PythonOperator(
        task_id='calculate_stats',
        python_callable=_calculate_states,
        op_kwargs={
            "input_paths": [
                "/data/09_data_aware/events/09c/{{triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start | ds }}.json",
                "/data/09_data_aware/events/09d/{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start | ds }}.json"
            ],
            "output_path": "/data/09_data_aware/stats/{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start | ds }}.csv",
        }
    )

    calculate_stats
