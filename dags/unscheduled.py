from pathlib import Path

import pandas
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def _calculate_states(input_path, output_path):
    """Calculates event statistics."""
    events = pandas.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

with DAG(
        dag_id='unscheduled',
        schedule=None,
):
    fetch_events = BashOperator(
        task_id='fetch_events',
        bash_command=(
            'mkdir -p /data && '
            "curl -o /data/events.json http://events-api:8081/latest"
        )
    )

    calculate_stats = PythonOperator(
        task_id='calculate_stats',
        python_callable=_calculate_states,
        op_args={
            'input_path': '/data/events.json',
            'output_path': '/data/stats.csv',
        }
    )

    fetch_events >> calculate_stats
