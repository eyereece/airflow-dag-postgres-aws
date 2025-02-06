import csv
import pika
import json
import os

from datetime import datetime, timedelta
from log import log_msg

from dotenv import load_dotenv
load_dotenv()

current_date = datetime.now()
previous_date = (current_date.replace(day=1) - timedelta(days=1))
year = previous_date.year
month = previous_date.month

CSV_FILE_PATH = f'/path/to/airflow/scripts/cleaned_data/clean_df_{year}_{month:02}.csv'
CSV_FILENAME = f'clean_df_2024_09.csv'

# Load for env
QUEUE_NAME = 'csv_queue'
QUEUE_HOST = 'localhost'

class CsvTaskQueue():
    '''
    Read from a CSV file, serialize, and publish to the given queue
    '''
    def __init__(self, csv_file_path, csv_filename, queue_name, queue_host):
        self.queue_name = queue_name
        self.queue_host = queue_host
        self.csv_filename = csv_filename
        self.rows = iter(csv.reader(csv_file_path))
        # Skip the header row
        next(self.rows, None)

    def send(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.queue_host))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)
        log_msg(f'[Pub] Declared queue: {self.queue_name}')

        for row in self.rows:
            channel.basic_publish(exchange='',
            routing_key=self.queue_name,
            body=json.dumps(row),
            properties=pika.BasicProperties(
                delivery_mode=2
            ))
        print(f'Sent {self.queue_name}')
        log_msg(f'[Pub] Sent all rows to {self.queue_name}')
        connection.close()

if __name__ == '__main__':
    try:
        with open(CSV_FILE_PATH) as csvfile:
            csv_task_queue = CsvTaskQueue(csv_file_path=csvfile,
                                            csv_filename=CSV_FILENAME,
                                            queue_name=os.getenv("QUEUE_NAME"),
                                            queue_host=os.getenv("QUEUE_HOST")
            )
            csv_task_queue.send()
    except Exception as e:
        log_msg(f'[Pub] Error: {e}', level='warning')
