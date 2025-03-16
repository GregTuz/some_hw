import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import random
from faker import Faker
from pymongo import MongoClient

fake = Faker()

import logging

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('ETL_logger')
logger.setLevel(logging.INFO)

def connect_to_mongo():
	client = MongoClient("mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL")
	db = client['ETL_MONGO_FINAL']

	return db

def write_to_mongo(collection_name, data):
	db = connect_to_mongo()
	collection = db[collection_name]
	collection.insert_many(data)
	logger.info(f"Data written to {collection_name} collection in MongoDB.")

def generate_user_sessions():
	data = [
		{
			"session_id": fake.uuid4(),
			"user_id": random.randint(1, 1000),
			"start_time": fake.iso8601(),
			"end_time": fake.iso8601(),
			"pages_visited": [fake.uri() for _ in range(random.randint(1, 5))],
			"device": fake.user_agent(),
			"actions": [fake.word() for _ in range(random.randint(1, 5))]
		}
		for _ in range(10000)
	]
	write_to_mongo("UserSessions", data)

def generate_product_price_history():
	data = [
		{
			"product_id": random.randint(1, 500),
			"price_changes": [{"date": fake.date(), "price": round(random.uniform(10, 500), 2)} for _ in range(5)],
			"current_price": round(random.uniform(10, 500), 2),
			"currency": "USD"
		}
		for _ in range(10000)
	]
	write_to_mongo("ProductPriceHistory", data)

def generate_event_logs():
	data = [
		{
			"event_id": fake.uuid4(),
			"timestamp": fake.iso8601(),
			"event_type": fake.word(),
			"details": fake.sentence()
		}
		for _ in range(10000)
	]
	write_to_mongo("EventLogs", data)

def generate_support_tickets():
	data = [
		{
			"ticket_id": fake.uuid4(),
			"user_id": random.randint(1, 1000),
			"status": random.choice(["open", "closed", "pending"]),
			"issue_type": fake.word(),
			"messages": [fake.sentence() for _ in range(random.randint(1, 5))],
			"created_at": fake.iso8601(),
			"updated_at": fake.iso8601()
		}
		for _ in range(10000)
	]
	write_to_mongo("SupportTickets", data)

def generate_user_recommendations():
	data = [
		{
			"user_id": random.randint(1, 1000),
			"recommended_products": [random.randint(1, 500) for _ in range(5)],
			"last_updated": fake.iso8601()
		}
		for _ in range(10000)
	]
	write_to_mongo("UserRecommendations", data)

def generate_moderation_queue():
	data = [
		{
			"review_id": fake.uuid4(),
			"user_id": random.randint(1, 1000),
			"product_id": random.randint(1, 500),
			"review_text": fake.text(),
			"rating": random.randint(1, 5),
			"moderation_status": random.choice(["pending", "approved", "rejected"]),
			"flags": [fake.word() for _ in range(random.randint(0, 3))],
			"submitted_at": fake.iso8601()
		}
		for _ in range(10000)
	]
	write_to_mongo("ModerationQueue", data)

def generate_search_queries():
	data = [
		{
			"query_id": fake.uuid4(),
			"user_id": random.randint(1, 1000),
			"query_text": fake.sentence(),
			"timestamp": fake.iso8601(),
			"filters": [fake.word() for _ in range(random.randint(0, 3))],
			"results_count": random.randint(0, 50)
		}
		for _ in range(10000)
	]
	write_to_mongo("SearchQueries", data)

with DAG(
		'Create_synth_data_for_mongo',
		default_args={
			'owner': 'admin',
			'depends_on_past': False,
			'start_date': datetime.datetime(2025, 3, 15),
			'retries': 1,
			'retry_delay': datetime.timedelta(minutes=5),
		},
		schedule_interval=None,
		catchup=False,
) as dag:

	PythonOperator(task_id="generate_user_sessions", python_callable=generate_user_sessions, dag=dag)
	PythonOperator(task_id="generate_product_price_history", python_callable=generate_product_price_history, dag=dag)
	PythonOperator(task_id="generate_event_logs", python_callable=generate_event_logs, dag=dag)
	PythonOperator(task_id="generate_support_tickets", python_callable=generate_support_tickets, dag=dag)
	PythonOperator(task_id="generate_user_recommendations", python_callable=generate_user_recommendations, dag=dag)
	PythonOperator(task_id="generate_moderation_queue", python_callable=generate_moderation_queue, dag=dag)
	PythonOperator(task_id="generate_search_queries", python_callable=generate_search_queries, dag=dag)
