from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

import psycopg2
import os

load_dotenv()


# Default Arguments for the DAG
default_args = {
    'owner': 'owner_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 16, 17, 20, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    'articles_mq_dag',
    default_args=default_args,
    description='Articles MQ DAG',
    schedule_interval='20 17 16 * *'
) as dag:

    # Task to activate the virtual environment and run the spider
    run_scraper = BashOperator(
        task_id='run_scraper',
        bash_command='source /path/to/airflow/airflow_venv/bin/activate &&'
                    'cd /path/to/airflow/scripts/articles_collector/articles_collector &&'
                    f'export http_proxy={os.getenv("HTTP_PROXY")} &&'
                    f'export https_proxy={os.getenv("HTTPS_PROXY")} &&'
                    'scrapy crawl first_spider'
    )

    run_data_transform = BashOperator(
        task_id='run_data_transform',
        bash_command='cd /path/to/airflow/scripts &&'
                    'python3 transform_data.py'
    )

    run_task_queue = BashOperator(
        task_id='run_task_queue',
        bash_command='cd /path/to/airflow/scripts/message_queue &&'
                    'python3 csv_task_queue.py'
    )

    run_db_worker = BashOperator(
        task_id='run_db_worker',
        bash_command='cd /path/to/airflow/scripts/message_queue &&'
                    'python3 db_worker.py'
    )

    run_scraper >> run_data_transform >> run_task_queue >> run_db_worker