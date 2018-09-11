from __future__ import print_function
import airflow
from airflow import DAG
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from datetime import datetime, timedelta
from airflow.models import Variable

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

tmpl_search_path = Variable.get("sql_path")

dag = airflow.DAG(
    'process_order_fact',
    schedule_interval="* * * * *",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1)

process_order_fact = PostgresOperatorWithTemplatedParams(
    task_id='process_order_fact',
    postgres_conn_id='orders_redshift',
    sql='query.sql',
    parameters={"cust_id": "CUST-001"},
    dag=dag,
    pool='redshift_dwh')
