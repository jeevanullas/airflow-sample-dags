from __future__ import print_function
import airflow
from airflow import DAG
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

tmpl_search_path = Variable.get("sql_path")

dag = airflow.DAG(
    'process_pipeline',
    schedule_interval="* * * * *",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1)

process_product_stg = PostgresOperatorWithTemplatedParams(
    task_id='process_product_into_staging',
    postgres_conn_id='orders_redshift',
    sql='prepare_staging_query.sql',
    parameters={"cust_id": "CUST-001"},
    dag=dag,
    pool='redshift_dwh')

extract_product_stg = RedshiftToS3Transfer(
    task_id='extract_product_stg_s3',
    schema='orders',
    table='staging_product',
    s3_bucket='bigdataset-airflow',
    s3_key='product',
    redshift_conn_id='orders_redshift',
    aws_conn_id='aws_conn_id',
    dag=dag,
    include_header=True)

process_product_stg >> extract_product_stg
