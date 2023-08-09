from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime as dt

def print_error():
    print('Error loi roi')
default_args={
    'on_failure_callback':print_error
}

with DAG(
    dag_id='Chap_12',
    start_date=dt.datetime(2023,7,4),
    default_args={"email": "bob@work.com"},
    schedule_interval='@daily'
) as dag:
    # task1=BashOperator(
    #     task_id='task1',
    #     bash_command='echo task1'
    # )
    # task2=BashOperator(
    #     task_id='task2',
    #     bash_command='ech1 task2'
    # )
    # task1>>task2
    failing_task = BashOperator(task_id="failing_task", bash_command="exit 1", dag=dag)