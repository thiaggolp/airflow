from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import datetime
import os
import logging
import pandas as pd

# --- Configurações ---
BASE_LOCAL_PATH = "/host_files/Arquivos Case"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=3),
}

# --- DAG ---
with DAG(
    dag_id="daily_user_engagement_report",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Executa às 6h da manhã
    start_date=days_ago(1),
    catchup=False,
    tags=["engajamento", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task()
    def check_file():
        date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        full_path = os.path.join(BASE_LOCAL_PATH, f"{date_str}.csv")
        logging.info(f"Verificando arquivo local: {full_path}")
        
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Arquivo {full_path} não encontrado.")
        return full_path

    @task()
    def download_file(filepath: str):
        logging.info(f"Simulando download do arquivo: {filepath}")
        return filepath  # Simulação: arquivo já está disponível localmente

    @task()
    def load_to_warehouse(file_path: str):
        df = pd.read_csv(file_path)
        logging.info(f"{len(df)} registros carregados. Simulando carga no data warehouse...")
        return True

    @task()
    def run_dbt_job():
        logging.info("Simulando execução do dbt...")
        return True

    @task()
    def validate_kpis():
        logging.info("Simulando validação dos KPIs...")
        # Exemplo de checagem real:
        # if df['views'].isnull().any():
        #     raise ValueError("Valores nulos detectados")
        return "KPIs validados"

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=lambda: print("✅ Slack mock: DAG finalizada com sucesso!")
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=lambda: print("❌ Slack mock: DAG falhou em alguma etapa."),
        trigger_rule="one_failed"
    )

    end = EmptyOperator(task_id="end")

    # --- Encadeamento ---
    file_path = check_file()
    loaded = load_to_warehouse(file_path)
    transformed = run_dbt_job()
    validated = validate_kpis()

    start >> file_path >> loaded >> transformed >> validated >> [notify_success, notify_failure] >> end