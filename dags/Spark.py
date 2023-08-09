from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,date
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import subprocess
from Spark.operator import SparkSubmit



with DAG(
    dag_id='A_Process_data_hive_spark',
    start_date=datetime(2015,2,1),
    end_date=datetime(2015,2,3),
    schedule='@daily'
) as dag:
    insert_data=SparkSubmit(
    task_id='Insert_data_hive', 
    application='plugins/scripts/project_Hive_Spark/untitled_2.12-0.1.0-SNAPSHOT.jar',
    conn_id='Cluster_spark',
    driver_memory='500m',
    status_poll_interval=1
    )

# templates_dict={
#         'start_date':date(2015,2,1) ,
#         'end_date':date(2015,2,2),
#         'schema':'transaction',
#         'table_name':'Transaction_History'
# }
# Insert_data(templates_dict)
    # application_args=['--start_date','{{ds}}','--end_date','{{next_ds}}','--data_path','data/archive/transactions.csv'],