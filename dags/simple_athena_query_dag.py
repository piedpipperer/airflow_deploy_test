from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

from datetime import datetime
#from airflow.providers.amazon.aws.operators.athena
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator

from airflow.macros import ds_format


default_args = {
    'owner': 'Airflow',
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



with DAG(dag_id='simple_athena_query',
         start_date=datetime(2019, 12, 5)
		 ,schedule_interval="@once" 
		 ,default_args=default_args) as dag:
		
#    month_nodash = ds_format( "{{ ds_nodash }}" , "%Y%m%d", "%Y%m") 
#    echo_string = "echo " + month_nodash #[:-2]	
#
#    t1_doc = BashOperator(
#        task_id='print_month'
#        ,bash_command=echo_string
#    )

	folder_month_delete = S3DeleteObjectsOperator(
        task_id='folder_month_delete'
        ,bucket="matchestest"
        ,keys="/athena/matches_monthly/month=201912"  #+ str({{ ds_nodash }}[:-2]))
        ,dag=dag
    )
	
#   run_query = AWSAthenaOperator(
#       task_id='run_query',
#       query="select * from UNNEST(SEQUENCE(DATE('2019-05-01'), date_trunc('day', DATE('{{ ds }}')), INTERVAL '1' DAY))",
#       output_location='s3://matchestest/airflow_athena/',
#       database='sampledb'
#   )
	
##task_one >> task_two >> [task_two_1, task_two_2, task_two_3] >> end