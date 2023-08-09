import requests
from airflow.hooks.base import BaseHook
from airflow import DAG
import datetime as dt
from airflow.operators.python import PythonOperator


class MovielensHook(BaseHook):
    def __init__(self,conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._session = None
        self._base_url=None
    def get_conn(self):
        if self._session is None:
            session=requests.Session()
            config=self.get_connection(self._conn_id)
            print("config ne!!!!!!!!!!!!!!!!!",config)
            schema=config.schema
            host=config.host
            port=config.port
            self._base_url=f"{schema}://{host}:{port}"
            self._session=session
        return self._session,self._base_url
    def get_ratings(self,start_date, end_date, batch_size=100):
        x=list(self.get_with_pagination(
            endpoint="/ratings",
            params={"start_date":start_date,"end_date":end_date},
            batch_size=batch_size
        ))
        return x

    def get_with_pagination(self,endpoint, params, batch_size=100):
        """
        Fetches records using a get request with given url/params,
        taking pagination into account.
        """
        session,base_url=self.get_conn()
        url=base_url+endpoint
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