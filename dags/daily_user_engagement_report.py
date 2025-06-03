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
BUCKET_NAME = "streaming-data"
FILENAME_TEMPLATE = "engagement_data/{date}.csv"
LOCAL_PATH = "/opt/airflow/tmp/"

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
        filename = FILENAME_TEMPLATE.format(date=date_str)
        logging.info(f"Verificando arquivo: {filename}")
        # Simula a verificação (substitua pelo S3Hook real se necessário)
        filepath = f"/opt/airflow/include/{filename.split('/')[-1]}"
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo {filepath} não encontrado.")
        return filepath

    @task()
    def download_file(filepath: str):
        logging.info(f"Simulando download do arquivo: {filepath}")
        return filepath  # Simulação: já está disponível localmente

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
        logging.info("Simulando validação de KPIs...")
        # Simulação de validação
        views_ok = True
        duration_ok = True
        if not (views_ok and duration_ok):
            raise ValueError("Falha na validação dos KPIs")
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
    path = check_file()
    local = download_file(path)
    loaded = load_to_warehouse(local)
    transformed = run_dbt_job()
    validated = validate_kpis()

    start >> path >> local >> loaded >> transformed >> validated >> [notify_success, notify_failure] >> end