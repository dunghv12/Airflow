from datetime import datetime,timedelta
from airflow.decorators import dag,task


default_args={
    'retries':3,
    'retry_delay':timedelta(seconds=5)
}

@dag(
    start_date=datetime(2023,6,24),
    schedule_interval='@daily',
    default_args=default_args,
    dag_id='PyOperator_v2'
)
def compute():
    @task()
    def double_up():
        x=2
        return x
    
    @task()
    def square(x):
        return x**x
    
    @task()
    def p_result(x):
        print('gia tri',x)
    
    task1=double_up()
    task2=square(task1)
    p_result(task2)
result=compute()

    