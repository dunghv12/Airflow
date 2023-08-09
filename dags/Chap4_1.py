from datetime import datetime,timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag=DAG(
    dag_id='Chap4_2',
    start_date=datetime(2023,6,28),
    schedule='@daily'
    )

get_data=BashOperator(
    dag=dag,
    task_id='get_data',
    bash_command="curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year -2 }}/"
        "{{ execution_date.year -2 }}-{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year -2 }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
)
extract_gz = BashOperator(
    task_id="extract_gz", bash_command="gunzip --force /tmp/wikipageviews.gz", dag=dag
)

def _fetch_pageviews(pagenames):
    result=dict.fromkeys(pagenames,0)
    with open ('/tmp/wikipageviews','r') as f:
        for line in f:
            domain,page_title,count,_=line.split(' ')
            if domain=='en' and page_title in pagenames:
                result[page_title]=count
        print(result)
fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)
get_data>>extract_gz>>fetch_pageviews
