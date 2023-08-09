import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


SWITCH_ERP=airflow.utils.dates.days_ago(1)
def _fech_sales_old():
    print('fetch sale old')

def _fech_sales_new():
    print('fetch sale new')

def _fetch_sales(**context):
    if context['execution_date']<SWITCH_ERP:
        print(SWITCH_ERP)
        _fech_sales_old()
    else:
        _fech_sales_new()

def _clean_sales_old():
    print('clean sale old')

def _clean_sales_new():
    print('clean sale new')

def _clean_sales(**context):
    if context['execution_date']<SWITCH_ERP:
        _clean_sales_old()
    else:
        _clean_sales_new()

with DAG(
    dag_id="Chap5_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales = PythonOperator(
        task_id="fetch_sales",
        python_callable=_fetch_sales
        )
    clean_sales = PythonOperator(
        task_id="clean_sales",
        python_callable=_clean_sales
    )

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")
    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model