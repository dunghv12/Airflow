import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
from pathlib import Path
import glob 

default_args={
    'retries':5,
    'retry_delay':timedelta(seconds=10),
    'owner': 'HoangDung'
}

dag = DAG(
    dag_id="Chap_6.4",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor.",
    default_args=default_args,
    concurrency=5
)

def _check_file():
    path_file=Path('/tmp')
    file_test=glob.glob("/tmp/*.csx")
    full_file=path_file/'test3.csv'
    print(file_test)
    return file_test and full_file.exists() 

check_file=PythonSensor(
    task_id='check_file',
    dag=dag,
    python_callable=_check_file,
    poke_interval=10,
    mode='reschedule'
    # timeout=10
)


