import json
import os
import time
import pika
import psycopg2
import traceback

from log import log_msg
from dotenv import load_dotenv

load_dotenv()

EXPECTED_COLUMNS = [
    'author', 'title', 'collection', 'read_time', 'claps', 'responses',
    'published_date', 'pub_year', 'pub_month', 'pub_date', 'pub_day',
    'word_count', 'title_cleaned', 'week', 'log_claps', 'word_count_title'
]

class DbWorker:
    def __init__(self, queue_host, queue_name, db_name, db_host, db_user, db_password, db_port):
        self.queue_host = queue_host
        self.queue_name = queue_name

        # Initialize Postgres connection
        self.conn = psycopg2.connect(
            database=db_name,
            host=db_host,
            user=db_user,
            password=db_password,
            port=db_port
        )
        log_msg(f'[Sub] Initialized database connection to {db_name}')

    def queue_is_empty(self, channel):
        # Check the number of messages in the queue
        queue = channel.queue_declare(queue=self.queue_name, durable=True, passive=True)
        return queue.method.message_count == 0

    def run(self):
        # Initialize RabbitMQ Connection
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.queue_host))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)
        log_msg(f'[Sub] Declared queue: {self.queue_name} for consumption')

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)
        log_msg("[Sub] Waiting for message ...")
        channel.start_consuming()

    def valid_entry(self, entry):
        """
        Make sure that the given entry from RabbitMQ can be placed in the db
        """
        if isinstance(entry, list) and len(entry) == len(EXPECTED_COLUMNS):
            return True
        return False

    def callback(self, ch, method, properties, body):
        """
        Callback function to process each message from RabbitMQ
        """
        # Parse message
        body = json.loads(body)
        # log_msg(f'[Sub] Received message: {body}')

        if self.valid_entry(body):
            try:
                sql_query = f"""
                INSERT INTO articles ({', '.join(EXPECTED_COLUMNS)})
                VALUES ({', '.join(['%s'] * len(EXPECTED_COLUMNS))})
                """

                # Execute SQL statement
                with self.conn.cursor() as cur:
                    cur.execute(sql_query, body)
                    self.conn.commit()
                # print("Data inserted successfully.")
                # log_msg("[Sub] Data inserted successfully.")

            except psycopg2.IntegrityError as e:
                # Rollback in case of an error
                print(f"Database error: {e}")
                log_msg(f"[Sub] Database error: {e}", level='warning')
                self.conn.rollback()
            except Exception as e:
                print(f"Unexpected Error: {e}")
                log_msg(f"[Sub] Unexpected Error: {e}", level='warning')
                traceback.print_exc()
                self.conn.rollback()
        else:
            print("Received invalid data format.")
            log_msg("[Sub] Received invalid data format.", level='warning')
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Check if the queue is empty and stop consuming if it is
        if self.queue_is_empty(ch):
            ch.stop_consuming()
            print("Data inserted successfully...")
            print("Queue is empty. Exiting.")
            log_msg("[Sub] Queue is empty. Exiting.")


if __name__ == '__main__':
    time.sleep(10)
    db_worker = DbWorker(
        queue_host=os.getenv("QUEUE_HOST"),
        queue_name=os.getenv("QUEUE_NAME"),
        db_name=os.getenv("POSTGRES_DB2"),
        db_host=os.getenv("POSTGRES_HOST"),
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PWD"),
        db_port=os.getenv("POSTGRES_PORT")
    )
    db_worker.run()
