import json
import pathlib
from typing import List, Any

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from requests.exceptions import ConnectionError, MissingSchema


def _get_pictures() -> None:
    # Ensure directory exists
    pathlib.Path('data/images').mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open('/tmp/launches.json') as f:

        launches: Any = json.load(f)
        image_urls: List[str] = [launch['image'] for launch in launches['results']]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f'data/images/{image_filename}'

                with open(target_file, 'wb') as fs:
                    fs.write(response.content)
                    print(f'Downloaded {image_url} to {target_file}')

            except MissingSchema:
                print(f'{image_url} appears to be invalid URL.')

            except ConnectionError:
                print(f'Could not connect to {image_url}.')


with DAG(
        dag_id='01_download_rocket_launches',
        start_date=pendulum.today('UTC').add(days=-14),
        schedule="*/30 * * * *"
):
    download_launches = BashOperator(
        task_id='download_launches',
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    get_pictures = PythonOperator(
        task_id='get_pictures',
        python_callable=_get_pictures,
    )

    notify = BashOperator(
        task_id='notify_closing',
        bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    )

    download_launches >> get_pictures >> notify
