from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag=DAG(
    dag_id="Chap3_1",
    start_date=datetime(2023,6,26),
    schedule_interval='@daily'
)

# down_events=BashOperator(
#     task_id='down_events',
#     bash_command='curl -o /tmp/events.json -L "https:/ /localhost:5000/events" ',
#     dag=dag
# )

def test(name,date_test):
    print("{date_test}")
    print("{name} is testing")

test=PythonOperator(
    task_id='test',
    python_callable=test,
    op_kwargs={
        'name':'Dung',
        'date_test':'{{ds}}'
    },
    dag=dag
)
# down_events