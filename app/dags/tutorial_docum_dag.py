# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['jobbing.314@gmail.com'],
    'email_on_failure': ['jobbing.314@gmail.com'],
    'email_on_retry': ['jobbing.314@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag_doc = DAG(
    'tutorial_docum', default_args=default_args, schedule_interval=timedelta(days=1))
	
t1_doc = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag_doc)

t2_doc = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag_doc)