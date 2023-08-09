from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
import os


dag=DAG(
    start_date=datetime(2023,6,26),
    schedule='@daily',
    dag_id='Chap2_version_1'
)

download_image=BashOperator(
    task_id='download_image',
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

def _get_images():
    if not os.path.exists('/tmp/images'):
        os.makedirs('/tmp/images')
    with open('/tmp/launches.json') as f:
        launches=json.load(f)
        links_images=[link['image'] for link in launches["results"]]
        for link in links_images:
            response=requests.get(link)
            name_image=link.split('/')[-1]
            target_file='/tmp/images/{}'.format(name_image)
            try:
                with open(target_file,'wb') as f:
                    f.write(response.content)
            except:
                print("Link is died",link)
            
get_images=PythonOperator(
    task_id='get_images',
    python_callable=_get_images,
    dag=dag
)

noti=BashOperator(
    task_id='notification',
    bash_command='echo Number of images is $(ls /tmp/images/ | wc -l)',
    dag=dag
)
download_image >> get_images >> noti

