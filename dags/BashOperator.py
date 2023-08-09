from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args={
    'retries':1,
    'retry_delay':timedelta(seconds=2),
    'owner': 'HoangDung'
}

with DAG(
    dag_id='Test_BashOperator',
    description='HD test',
    default_args=default_args,
    start_date=datetime(2023,7,3),
    schedule_interval='12 06 * * *'
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo Task1 is running'
    )
    
    task2=BashOperator(
        task_id='task2',
        bash_command='echo Task2 is running')
    
    task3=BashOperator(
        task_id='task3',
        bash_command='echo Task3 is running')
    
    task4=BashOperator(
        task_id='task4',
        bash_command='echo Task4 is running')
    
    task1>>[task2,task3]>>task4
    # task1>>task3>>task4
