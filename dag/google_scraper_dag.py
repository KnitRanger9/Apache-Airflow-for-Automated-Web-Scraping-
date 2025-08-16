from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from bs4 import BeautifulSoup
import requests
import base64
import os
from datetime import datetime, timedelta
import json

# Configuration
GOOGLE_NEWS_URL = "https://news.google.com"
SCRAPE_OUTPUT_DIR = "/path/to/your/dags/run"  # Update this path
STATUS_FILE_PATH = os.path.join(SCRAPE_OUTPUT_DIR, "status")

# Database configuration
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_TABLE_HEADLINES = "headlines"
POSTGRES_TABLE_IMAGES = "images"

EMAIL_RECIPIENT = "your_email@example.com"

# Module 1: Scrape Google News Home Page
def scrape_google_news_home():
    response = requests.get(GOOGLE_NEWS_URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        return soup
    else:
        raise Exception(f"Failed to fetch Google News: {response.status_code}")

# Module 2: Scrape Top Stories
def scrape_top_stories(soup):
    top_stories = []
    for article in soup.find_all("article"):
        link = article.find("a")
        if link:
            top_stories.append({
                "url": GOOGLE_NEWS_URL + link["href"],
                "title": link.text.strip()
            })
    return top_stories

# Module 3: Extract Thumbnail and Headline
def extract_thumbnail_and_headline(top_stories):
    results = []
    for story in top_stories:
        response = requests.get(story["url"])
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            thumbnail = soup.find("img")
            if thumbnail:
                results.append({
                    "headline": story["title"],
                    "thumbnail": base64.b64encode(requests.get(thumbnail["src"]).content).decode("utf-8"),
                    "url": story["url"],
                    "scrape_timestamp": datetime.now().isoformat()
                })
    return results

# Module 4: Store Data in Postgres
def store_data_in_postgres(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract_thumbnail_and_headline")
    if not data:
        raise ValueError("No data to store")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Insert data into Postgres
    insert_headlines_query = f"""
        INSERT INTO {POSTGRES_TABLE_HEADLINES} (headline, url, scrape_timestamp)
        VALUES (%s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
    """
    insert_images_query = f"""
        INSERT INTO {POSTGRES_TABLE_IMAGES} (url, image_base64)
        VALUES (%s, %s)
        ON CONFLICT (url) DO NOTHING;
    """

    successful_inserts = 0
    for entry in data:
        # Insert headline
        headline_values = (entry["headline"], entry["url"], entry["scrape_timestamp"])
        kwargs["postgres_conn_id"].run(insert_headlines_query, parameters=headline_values)
        # Insert image
        image_values = (entry["url"], entry["thumbnail"])
        kwargs["postgres_conn_id"].run(insert_images_query, parameters=image_values)
        successful_inserts += 1

    conn.commit()
    cur.close()
    conn.close()

    # Write status file
    with open(STATUS_FILE_PATH, "w") as f:
        f.write(str(successful_inserts))

    return successful_inserts

def prepare_email_content(**kwargs):
    ti = kwargs['ti']
    successful_inserts = ti.xcom_pull(task_ids='store_data_in_postgres')
    
    email_body = f"""
    <html>
    <body>
    <h2>Google News Scraping Results</h2>
    <p>The scraping process has completed successfully.</p>
    <p>Number of articles inserted: {successful_inserts}</p>
    </body>
    </html>
    """
    return email_body

# DAG Definition
default_args = {
    "owner": "Aritra",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": EMAIL_RECIPIENT,
}

scrape_and_store_dag = DAG(
    "scrape_and_store_dag",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
)

# Tasks
scrape_home_task = PythonOperator(
    task_id="scrape_google_news_home",
    python_callable=scrape_google_news_home,
    provide_context=True,
    dag=scrape_and_store_dag,
)

scrape_top_stories_task = PythonOperator(
    task_id="scrape_top_stories",
    python_callable=scrape_top_stories,
    op_args=[scrape_home_task.output],
    dag=scrape_and_store_dag,
)

extract_thumbnail_and_headline_task = PythonOperator(
    task_id="extract_thumbnail_and_headline",
    python_callable=extract_thumbnail_and_headline,
    op_args=[scrape_top_stories_task.output],
    dag=scrape_and_store_dag,
)

setup_postgres_tables_task = PostgresOperator(
    task_id="setup_postgres_tables",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_HEADLINES} (
            headline TEXT,
            url TEXT PRIMARY KEY,
            scrape_timestamp TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE_IMAGES} (
            url TEXT PRIMARY KEY,
            image_base64 TEXT
        );
    """,
    dag=scrape_and_store_dag,
)

store_data_task = PythonOperator(
    task_id="store_data_in_postgres",
    python_callable=store_data_in_postgres,
    provide_context=True,
    dag=scrape_and_store_dag,
)

prepare_email_content_task = PythonOperator(
    task_id='prepare_email_content',
    python_callable=prepare_email_content,
    provide_context=True,
    dag=scrape_and_store_dag,
)

send_email_task = EmailOperator(
    task_id='send_result_email',
    to=EMAIL_RECIPIENT,
    subject='Google News Scraping Results',
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email_content') }}",
    dag=scrape_and_store_dag,
)

# Task Dependencies
scrape_home_task >> scrape_top_stories_task >> extract_thumbnail_and_headline_task
setup_postgres_tables_task >> store_data_task >> prepare_email_content_task >> send_email_task
extract_thumbnail_and_headline_task >> store_data_task
