import json
import pathlib
from typing import Any, List

import requests
from airflow.providers.standard.operators.python import PythonOperator
from requests.exceptions import MissingSchema

def _get_pictures():
    # Ensure directory exists
    pathlib.Path('/tmp/images').mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open('/tmp/launches.json') as f:

        launches: Any = json.load(f)
        image_urls: List[str] = [launch['image'] for launch in launches['results']]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f'/tmp/images/{image_filename}'

                with open(target_file, 'wb') as fs:
                    fs.write(response.content)
                    print(f'Downloaded {image_url} to {target_file}')

            except MissingSchema:
                print(f'{image_url} appears to be invalid URL.')

            except ConnectionError:
                print(f'Could not connect to {image_url}.')

get_pictures = PythonOperator(
    task_id='get_pictures',
    python_callable=_get_pictures,
)
