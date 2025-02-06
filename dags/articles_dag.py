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
    'spider_dag',
    default_args=default_args,
    description='Scrapy Spider DAG',
    schedule_interval=None
) as dag:

    # Task to activate the virtual environment and run the spider
    run_scraper = BashOperator(
        task_id='run_scraper',
        bash_command='source /path/to/airflow_venv/bin/activate &&'
                    'cd /path/to/airflow/scripts/articles_collector/articles_collector &&'
                    f'export http_proxy={os.getenv("HTTP_PROXY")} &&'
                    f'export https_proxy={os.getenv("HTTPS_PROXY")} &&'
                    'scrapy crawl first_spider'
    )

    run_data_transform = BashOperator(
        task_id='run_data_transform',
        bash_command='cd /path/to/airflow/scripts &&'
                    'python transform_data.py'
    )

    run_copy_to_db = BashOperator(
        task_id='run_copy_to_db',
        bash_command='cd /path/to/airflow/scripts &&'
                    'python save_to_db.py'
    )

    run_scraper >> run_data_transform >> run_copy_to_db