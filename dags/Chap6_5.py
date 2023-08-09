from pathlib import Path

import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor

dag1 = DAG(
    dag_id="listing_6_04_dag01",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
)

def _wait_for_supermarket(supermarket_id_):
    supermarket_path = Path("/tmp")
    data_files = supermarket_path.glob("*.csv")
    success_file = supermarket_path / "test3.csv"
    return data_files and success_file.exists()


for supermarket_id in range(1, 5):
    wait = PythonSensor(
        task_id=f"wait_for_supermarket_{supermarket_id}",
        python_callable=_wait_for_supermarket,
        op_kwargs={"supermarket_id_": f"supermarket{supermarket_id}"},
        dag=dag1,
    )
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermarket_id}", dag=dag1)
    process = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag1)
    trigger_create_metrics_dag = TriggerDagRunOperator(
        task_id=f"trigger_create_metrics_dag_supermarket_{supermarket_id}",
        trigger_dag_id="listing_6_04_dag02",
        dag=dag1,
    )
    save_1= BashOperator(
        task_id=f"save_0_{supermarket_id}",bash_command="echo 5", dag=dag1)
    
    save_2 = BashOperator(
        task_id=f"save_1_{supermarket_id}",bash_command="echo 2", dag=dag1)

    wait >> copy >> process >> trigger_create_metrics_dag>>save_1>>save_2
dag2 = DAG(
    dag_id="listing_6_04_dag02",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

compute_differences = BashOperator(task_id="compute_differences",bash_command="sleep 10", dag=dag2)
update_dashboard = DummyOperator(task_id="update_dashboard", dag=dag2)
notify_new_data = DummyOperator(task_id="notify_new_data", dag=dag2)
compute_differences >> update_dashboard