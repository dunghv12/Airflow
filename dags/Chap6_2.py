import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta


default_args={
    'retries':5,
    'retry_delay':timedelta(seconds=10),
    'owner': 'HoangDung'
}

dag = DAG(
    dag_id="Chap_6.2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor.",
    default_args=default_args
)

wait = FileSensor(
    task_id="wait_for_supermarket_1", filepath="test.csv", dag=dag,
    fs_conn_id="connect_file_test_1",
    poke_interval=5 
)   
# wait=BashOperator(
#     task_id="wait_for_supermarket_",
#     dag=dag,
#     bash_command="echo d"
# )