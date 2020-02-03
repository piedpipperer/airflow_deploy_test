from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from datetime import datetime

with DAG(dag_id='simple_athena_query',
         schedule_interval=None,
         start_date=datetime(2019, 5, 21)) as dag:

    run_query = AWSAthenaOperator(
        task_id='run_query',
        query='SELECT * FROM  UNNEST(SEQUENCE(0, 100))',
        output_location='s3://matchestest/airflow_athena/',
        database='sampledb'
    )