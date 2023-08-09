import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
import datetime

dag1 = DAG(
    dag_id="Chap6_19_dag_1",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag2 = DAG(
    dag_id="Chap6_19_dag_2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag3 = DAG(
    dag_id="Chap6_19_dag_3",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
dag4 = DAG(
    dag_id="Chap6_19_dag_4",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 2 * * *",
)

BashOperator(task_id="etl",bash_command='sleep 5', dag=dag1)
BashOperator(task_id="etl",bash_command='sleep 5', dag=dag2)
BashOperator(task_id="etl",bash_command='sleep 5', dag=dag3)

test1=ExternalTaskSensor(
        task_id="wait_for_etl_dag1",
        external_dag_id="Chap6_19_dag_1",
        external_task_id="etl",
        execution_delta=datetime.timedelta(hours=2),
        dag=dag4,
    )
test2=ExternalTaskSensor(
        task_id="wait_for_etl_dag2",
        external_dag_id="Chap6_19_dag_2",
        external_task_id="etl",
        execution_delta=datetime.timedelta(hours=2),
        dag=dag4,
    )
test3=ExternalTaskSensor(
        task_id="wait_for_etl_dag3",
        external_dag_id="Chap6_19_dag_3",
        external_task_id="etl",
        execution_delta=datetime.timedelta(hours=2),
        dag=dag4,
    )
[test1,test2,test3] >> PythonOperator(task_id="report", dag=dag4, python_callable=lambda: print("hello"))