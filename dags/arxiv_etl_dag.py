from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import os

# Definir argumentos por defecto
default_args = {
    'owner': 'Edward',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Crear el DAG
with DAG(
    'arxiv_etl',
    default_args=default_args,
    description='ETL de ArXiv con carga a S3',
    schedule_interval='0 8,18 * * *',  # 8:00am y 6:00pm
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Ejecutar extracción de datos
    extract_task = BashOperator(
        task_id='extraer_datos',
        bash_command='python3 /path/to/scripts/extract_data.py'
    )

    # Task 2: Ejecutar transformación de datos
    transform_task = BashOperator(
        task_id='transformar_datos',
        bash_command='python3 /path/to/scripts/transform_data.py'
    )

    # Task 3: Cargar datos en la base de datos
    load_task = BashOperator(
        task_id='cargar_datos',
        bash_command='python3 /path/to/scripts/load_data.py'
    )

    # Task 4: Subir datos a S3
    upload_task = BashOperator(
        task_id='subir_a_s3',
        bash_command='python3 /path/to/scripts/upload_to_s3.py'
    )

    # Definir dependencias entre tareas
    extract_task >> transform_task >> load_task >> upload_task
