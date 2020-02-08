from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from datetime import datetime
#from airflow.providers.amazon.aws.operators.athena
from datetime import timedelta

default_args = {
    'owner': 'bet369',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['jobbing.314@gmail.com'],
    'email_on_failure': ['jobbing.314@gmail.com'],
    'email_on_retry': ['jobbing.314@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    #, 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


with DAG(dag_id='bet369_firstpart_goals_month'
         ,schedule_interval="@monthly"
         ,start_date=datetime(2019, 12, 5)
		 ,template_searchpath = ['/sqls/']
		 ,default_args=default_args) as dag:

    tmp_minuts_info0 = AWSAthenaOperator(
        task_id='00_tmp_minuts_info',
        query="00_month_tmp_minuts_info.sql",
		output_location='s3://matchestest/airflow_athena/logs',
        database='sampledb'
    )
	
	##task_one >> task_two >> [task_two_1, task_two_2, task_two_3] >> end