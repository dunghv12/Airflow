import datetime as dt
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="Chap10_fetch_date",
    description="Fetches ratings from the Movielens API susing Docker.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 12),   
    schedule_interval="@daily",
) as dag:
    fetch_data=DockerOperator(
        task_id='fetch_data',
        image='movielens-fetch:3.8.1',
        command=  [
            "python",
            "/usr/local/bin/fetch-ratings",
            "--start_date",
            "{{ds}}",
            "--end_date",
            "{{next_ds}}",
            "--output_path",
            "/data/{{ds}}.json",
        ],
        mounts=[Mount(source="/Users/hoangdung/Documents/Study/Airflow/data/movie", target="/data", type="bind")],
        mount_tmp_dir=False,
        network_mode="airflow_default"
        )
    
    ranking=DockerOperator(
            task_id='ranking_data',
            image='movielens-ranking:3.9',
            command=[
                "python",
                "/usr/local/bin/ranking_movie",
                "--input_path",
                "/data/{{ds}}.json",
                "--output_path",
                "/data/ranking/{{ds}}.csv"
            ],
            mounts=[
                Mount(source="/Users/hoangdung/Documents/Study/Airflow/data/movie",target="/data",type="bind"),
                Mount(source="/Users/hoangdung/Documents/Study/Airflow/data/ranking",target="/data/ranking",type="bind")],
            mount_tmp_dir=False,
            network_mode="airflow_default"

        )
    fetch_data>>ranking

    # [
    #         "fetch-ratings",
    #         "--start_date",
    #         "{{ds}}",
    #         "--end_date",
    #         "{{next_ds}}",
    #         "--output_path",
    #         "/data/ratings/{{ds}}.csv",
    #     ],