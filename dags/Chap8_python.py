import datetime as dt
import logging
import json
import os
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
# from custom.ranking import rank_movies_by_rating


MOVIELENS_HOST = os.environ.get("MOVIELENS_HOST", "movielens")
MOVIELENS_SCHEMA = os.environ.get("MOVIELENS_SCHEMA", "http")
MOVIELENS_PORT = os.environ.get("MOVIELENS_PORT", "5000")

# MOVIELENS_USER = os.environ["MOVIELENS_USER"]
# MOVIELENS_PASSWORD = os.environ["MOVIELENS_PASSWORD"]
def _get_ratings(start_date, end_date, batch_size=100):
    session,base_url=_get_session()
    x=list(_get_with_pagination(
        session=session,
        url=base_url+"/ratings",
        params={"start_date":start_date,"end_date":end_date},
        batch_size=batch_size
    ))
    return x
def _get_session():
    """Builds a requests Session for the Movielens API."""
    # Setup our requests session.
    session = requests.Session()
    host=MOVIELENS_HOST
    schema=MOVIELENS_SCHEMA
    port=MOVIELENS_PORT
    base_url=f"{schema}://{host}:{port}"
    return session,base_url


    # Define API base url from connection details.


def _get_with_pagination(session, url, params, batch_size=100):
    """
    Fetches records using a get request with given url/params,
    taking pagination into account.
    """
    offset=0
    total=None
    while total is None or offset < total:
        print(params)
        response=session.get(
            url,
            params={**params,"offset": offset, "limit": batch_size}
            )
        data=response.json()
        yield from data["result"]
        offset+=batch_size
        total=data["total"]
# start_date=dt.datetime(2019,1,1)
# end_date=dt.datetime(2019,1,2)
# start_date="2019-01-01"
# end_date= "2019-01-02"
# x=_get_ratings(start_date, end_date, batch_size=100)
with DAG(
    dag_id="Chap8_python",
    description="Fetches ratings from the Movielens API using the Python Operator.",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 10),
    schedule_interval="@daily",
) as dag:
    def _fetch_ratings(templates_dict):
        start_date=templates_dict['start_date']
        end_date=templates_dict['end_date']
        output_patch=templates_dict['output_path']
        print(start_date,end_date)
        data=_get_ratings(
            start_date=start_date,
            end_date=end_date,
            batch_size=100
        )
        outout_dir=os.path.dirname(output_patch)
        os.makedirs(outout_dir,exist_ok=True)
        with open(output_patch,"w") as f:
            json.dump(data,fp=f)
    fetch_ratings=PythonOperator(
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/tmp/movie/{{ds}}.json"
        },
        python_callable=_fetch_ratings,
        task_id="fetch_ratings"
    )

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


    ranking_movíe=PythonOperator(
        task_id='ranking_movíe',
        python_callable=_ranking_movie,
        templates_dict={
            "input_path": "/tmp/movie/{{ds}}.json",
            "output_path": "/tmp/movie/ranking/{{ds}}.json"
        }
    )


    fetch_ratings>>ranking_movíe
    

        

    
