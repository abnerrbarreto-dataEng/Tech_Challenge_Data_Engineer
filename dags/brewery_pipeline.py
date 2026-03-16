from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='brewery_data_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',      
    catchup=False,                   
    tags=['brewery', 'data_lake', 'medallion'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1, 
        # 'retry_delay': timedelta(minutes=5),
    }
) as dag:
    # Task 1: Extrair dados da API e carregar para a camada Bronze
    extract_bronze = BashOperator(
        task_id='extract_bronze_layer',
        bash_command='python /opt/airflow/scripts/extract_breweries.py --execution_date {{ ds }}',
    )

    # Task 2: Transformar dados da Bronze para a Silver
    transform_silver = BashOperator(
        task_id='transform_silver_layer',
        bash_command='bash /opt/airflow/scripts/run_pyspark.sh /opt/airflow/scripts/transform_silver.py {{ ds }}',
    )

    # Task 3: Agregar dados da Silver para a Gold
    aggregate_gold = BashOperator(
        task_id='aggregate_gold_layer',
        bash_command='bash /opt/airflow/scripts/run_pyspark.sh /opt/airflow/scripts/aggregate_gold.py {{ ds }}',
    )

    # Definir as dependências entre as tasks
    # extract_bronze -> transform_silver -> aggregate_gold
    extract_bronze >> transform_silver >> aggregate_gold