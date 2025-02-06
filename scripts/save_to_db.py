import psycopg2
import os
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

current_date = datetime.now()
previous_date = (current_date.replace(day=1) - timedelta(days=1))
year = previous_date.year
month = previous_date.month
input_file_path = f'/airflow/scripts/cleaned_data/clean_df_{year}_{month:02}.csv'
df = pd.read_csv(input_file_path)
try:
    try:
        conn = psycopg2.connect(
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PWD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        print("Connection successful!")
        print(os.getenv('POSTGRES_DB'))
    except Exception as e:
        print(f"Error connecting to the database: {e}")

    conn.autocommit = True
    cursor = conn.cursor()

    # Iterate through rows and insert into the database
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO articles (
                author, title, collection, read_time, claps, responses,
                published_date, pub_year, pub_month, pub_date, pub_day,
                word_count, title_cleaned, week, log_claps, word_count_title
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()

    print("Data successfully copied to database.")
except Exception as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()