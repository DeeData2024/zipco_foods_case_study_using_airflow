from datetiome import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extraction import run_extraction
from transformation import run_transformation
from loading import run_loading


#set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 13),
    'email': 'daisyshinny@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    'zipco_foods_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for Zipco Foods',
)

# define the individual tasks
extraction = PythonOperator(
    task_id='extraction layer',
    python_callable=run_extraction,
    dag=dag
)
transformation = PythonOperator(
    task_id='transformation layer',
    python_callable=run_transformation,
    dag=dag
)

loading = PythonOperator(
    task_id='loading layer',
    python_callable=run_loading,
    dag=dag
)

# set the task dependencies
extraction >> transformation >> loading