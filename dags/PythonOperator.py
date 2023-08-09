from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args={
    'retries':3,
    'retry_delay':timedelta(seconds=5)
}
def double(ti):
    x=2
    ti.xcom_push(key='key_x',value=x)
    return x
def pow(ti):
    y=ti.xcom_pull(task_ids='task1',key='key_x')
    print(y*y)
with DAG(
    dag_id='PythonOperator_v1',
    default_args=default_args,
    start_date=datetime(2023,6,25),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='task1',
        python_callable=double,
        op_kwargs={
            'x':2
        }
    )
    
    task2=PythonOperator(
        task_id='task2',
        python_callable=pow
    )
    task1>>task2