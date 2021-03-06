import airflow
from airflow import DAG
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

def process_customers_order_dim(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = "select distinct customer.cust_name, order_info.order_id from customer, order_info where customer.customer_id=order_info.customer_id;"

    records = pg_hook.get_records(sql)
    print(records)

    return records

dag = DAG(
    'process_dimensions',
    schedule_interval="* * * * *",
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

process_customers_order_dim = PythonOperator(
    task_id='process_customer_order',
    op_kwargs = {'conn_id':'orders_redshift'},
    python_callable=process_customers_order_dim,
    dag=dag)
