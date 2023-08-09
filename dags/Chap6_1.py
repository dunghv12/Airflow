import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="Chap6_1",
    start_date=airflow.utils.dates.days_ago(6),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor.",
    default_args={"depends_on_past": True},
    concurrency=1
)

create_metrics = DummyOperator(task_id="create_metrics", dag=dag)
process_metríc_2=DummyOperator(task_id="process_metríc_2", dag=dag)

for supermarket_id in [1, 2, 3, 4]: 
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermarket_id}", dag=dag)
    process = BashOperator(task_id=f"process_supermarket_{supermarket_id}",bash_command="echo hi", dag=dag)
    if supermarket_id!=2:
        copy >> process >> create_metrics
    else:
        copy >> process >> process_metríc_2>>create_metrics