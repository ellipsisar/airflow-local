from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def saludar():
    print("¡Hola desde Airflow en Docker!")

with DAG(
    dag_id="mi_primer_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ejemplo"],
) as dag:

    tarea = PythonOperator(
        task_id="saludar",
        python_callable=saludar,
    )