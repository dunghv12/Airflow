from Chap8.custom_operator import MovielensFetchRatingsOperator
import datetime as dt
from airflow import DAG


dag=DAG(dag_id="Chap8_CustomeOperator",
    description="Fetches ratings from the Movielens API using the Python Operator.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 10),
    schedule_interval="@daily")

fetch_ratings=MovielensFetchRatingsOperator(
    task_id='fetch_ratings',
    start_date="{{ds}}",
    end_date="{{next_ds}}",
    output_path="/tmp/movie/{{ds}}.json",
    conn_id="movielens",
    dag=dag
)
