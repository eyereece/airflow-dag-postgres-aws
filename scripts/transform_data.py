import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Calculate the previous month
current_date = datetime.now()
previous_date = (current_date.replace(day=1) - timedelta(days=1))
year = previous_date.year
month = previous_date.month
raw_file_path = f'/airflow/scripts/articles_collector/articles_collector/scraped_data/raw_data_{year}_{month:02}.csv'

df = pd.read_csv(raw_file_path)

def preprocess(text):
    return text.lower()

# Process DataFrame
df['published_date'] = pd.to_datetime(df['published_date'])
df['pub_year'] = df['published_date'].dt.year
df['pub_month'] = df['published_date'].dt.month
df['pub_date'] = df['published_date'].dt.day
df['pub_day'] = df['published_date'].dt.day_name()

# Fill missing values
df['claps'] = df['claps'].fillna(0)
df['responses'] = df['responses'].fillna(0)

# Calculate word count
avg_wpm = 265
df['word_count'] = df['read_time'] * avg_wpm

df['week'] = df['published_date'].dt.isocalendar().week

# Clean the title column
df['title'] = df['title'].fillna('no_title')
df['title_cleaned'] = df['title'].apply(preprocess)
df.drop(columns=['subtitle_preview', 'scraped_date'], inplace=True)

# Filter collections
collections_to_include = ['Towards Data Science', 'Towards AI', 'Javarevisited', 'Level Up Coding', 'Data Engineer Things']
df = df[df['collection'].isin(collections_to_include)]

# Logarithmic transformation of claps
df['log_claps'] = np.log1p(df['claps'])

# Calculate word count from titles
df['word_count_title'] = df['title_cleaned'].apply(lambda x: len(x.split()))

df['author'] = df['author'].astype(str)
df['title'] = df['title'].astype(str)
df['read_time'] = df['read_time'].astype(int)
df['claps'] = df['claps'].astype(int)
df['responses'] = df['responses'].astype(int)
df['pub_year'] = df['pub_year'].astype(int)
df['pub_month'] = df['pub_month'].astype(int)
df['pub_date'] = df['pub_date'].astype(int)
df['pub_day'] = df['pub_day'].astype(str)
df['word_count'] = df['word_count'].astype(int)
df['week'] = df['week'].astype(int)
df['published_date'] = df['published_date'].dt.date
df['title'] = df['title'].astype(str)
df['collection'] = df['collection'].astype(str)
df['log_claps'] = df['log_claps'].astype(float)
df['title_cleaned'] = df['title_cleaned'].astype(str)

# Create a column order to match postgres
col_order = ['author', 'title', 'collection', 'read_time', 'claps', 'responses', 'published_date', 'pub_year', 'pub_month', 'pub_date', 'pub_day', 'word_count', 'title_cleaned', 'week', 'log_claps', 'word_count_title']

# Reorder df columns to match
df = df[col_order]

print('AFTER')
print(df.info())

# Save cleaned DataFrame to CSV
output_file_path = f'/airflow/scripts/cleaned_data/clean_df_{year}_{month:02}.csv'
df.to_csv(output_file_path, index=False)