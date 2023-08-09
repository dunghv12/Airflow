import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator


SWITCH_ERP=airflow.utils.dates.days_ago(1)
def _fech_sales_old():
    print('fetch sale old')

def _fech_sales_new():
    print('fetch sale new')


def _clean_sales_old():
    print('clean sale old')

def _clean_sales_new():
    print('clean sale new')

def _pick_erp_system(**context):
    if context['execution_date']<SWITCH_ERP:
        return 'fetch_sales_old'
    else :
        return 'fetch_sales_new'

with DAG(
    dag_id="Chap5_2_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales_old = PythonOperator(
        task_id="fetch_sales_old",
        python_callable=_fech_sales_old
        )
    
    fetch_sales_new = PythonOperator(
        task_id="fetch_sales_new",
        python_callable=_fech_sales_new
        )
    
    clean_sales_old = PythonOperator(
        task_id="clean_sales_old",
        python_callable=_clean_sales_old
    )

    clean_sales_new = PythonOperator(
        task_id="clean_sales_new",
        python_callable=_clean_sales_new
    )

    pick_erp_system=BranchPythonOperator(
        task_id='pick_erp_system',
        python_callable=_pick_erp_system
    )

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")
    join_datasets = DummyOperator(task_id="join_datasets",trigger_rule="none_failed")
    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")
    join_erp = DummyOperator(task_id="join_erp_branch", trigger_rule="none_failed")

    start >> [pick_erp_system,fetch_weather]
    pick_erp_system>>[fetch_sales_old,fetch_sales_new]
    fetch_sales_old>>clean_sales_old
    fetch_sales_new>>clean_sales_new
    [clean_sales_new,clean_sales_old]>>join_erp
    fetch_weather>>clean_weather
    [join_erp, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model