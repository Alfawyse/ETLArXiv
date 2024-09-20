from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Edward',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'arxiv_etl',
    default_args=default_args,
    description='ETL process for ArXiv with load to S3',
    schedule_interval='0 8,18 * * *',  # 8:00am and 6:00pm
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python3 /ETLArXiv/scripts/extract_data.py'
    )

    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='python3 /ETLArXiv/scripts/transform_data.py'
    )

    # Comentar la tarea de load
    # load_task = BashOperator(
    #    task_id='load_data',
    #    bash_command='python3 /ETLArXiv/scripts/load_data.py'
    # )

    upload_task = BashOperator(
        task_id='upload_to_s3',
        bash_command='python3 /ETLArXiv/scripts/upload_to_s3.py'
    )

    extract_task >> transform_task >> upload_task
