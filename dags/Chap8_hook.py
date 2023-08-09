import requests
from airflow import DAG
import datetime as dt
from airflow.operators.python import PythonOperator
from Chap8.hooks import MovielensHook
import os
import json
import pandas as pd

dag=DAG(dag_id="Chap8_hook",
    description="Fetches ratings from the Movielens API using the Python Operator.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 10),
    schedule_interval="@daily")

def _fetch_ratings(conn_id,templates_dict):
    start_date=templates_dict['start_date']
    end_date=templates_dict['end_date']
    output_patch=templates_dict['output_path']
    print(start_date,end_date)
    hook=MovielensHook(conn_id)
    data=hook.get_ratings(
            start_date=start_date,
            end_date=end_date,
            batch_size=100
        )
    outout_dir=os.path.dirname(output_patch)
    os.makedirs(outout_dir,exist_ok=True)
    with open(output_patch,"w") as f:
        json.dump(data,fp=f)
def _ranking_movie(templates_dict):
        input_path = templates_dict["input_path"]
        data=pd.read_json(input_path)
        ranking=data.loc[data.rating>3,:].groupby(["movieId","rating"])\
                    .agg(
                        count_rating=pd.NamedAgg(column="rating", aggfunc="count"),
                    )\
                    .sort_values("count_rating", ascending=False)
        output_path = templates_dict["output_path"]
        output_dir=os.path.dirname(output_path)
        os.makedirs(output_dir,exist_ok=True)
        ranking.to_csv(output_path,index=True)

fetch_ratings=PythonOperator(
    templates_dict={
        "start_date": "{{ds}}",
        "end_date": "{{next_ds}}",
        "output_path": "/tmp/movie/{{ds}}.json"
    },
    python_callable=_fetch_ratings,
    op_kwargs={"conn_id":"movielens"},
    task_id="fetch_ratings",
    dag=dag
    )


ranking_movie=PythonOperator(
    task_id='ranking_movíe',
    python_callable=_ranking_movie,
    templates_dict={
        "input_path": "/tmp/movie/{{ds}}.json",
        "output_path": "/tmp/movie/ranking/{{ds}}.json"
    },
    dag=dag
)
fetch_ratings>>ranking_movie