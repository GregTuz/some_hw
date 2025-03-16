import sys
import pymongo
import pyspark.sql
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from pymongo import MongoClient
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, ArrayType, DecimalType

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('ETL_logger')
logger.setLevel(logging.INFO)

TABLE_SCHEMAS = {
	"UserSessions": {
		"_id": StringType(),
		"session_id": StringType(),
		"user_id": IntegerType(),
		"start_time": TimestampType(),
		"end_time": TimestampType(),
		"pages_visited": ArrayType(StringType()),
		"device": StringType(),
		"actions": ArrayType(StringType())
	},
	"ProductPriceHistory": {
		"_id": StringType(),
		"product_id": IntegerType(),
		"price_changes": StringType(),
		"current_price": DecimalType(10, 2),
		"currency": StringType()
	},
	"EventLogs": {
		"_id": StringType(),
		"event_id": StringType(),
		"timestamp": TimestampType(),
		"event_type": StringType(),
		"details": StringType()
	},
	"SupportTickets": {
		"_id": StringType(),
		"ticket_id": StringType(),
		"user_id": IntegerType(),
		"status": StringType(),
		"issue_type": StringType(),
		"messages": ArrayType(StringType()),
		"created_at": TimestampType(),
		"updated_at": TimestampType()
	},
	"UserRecommendations": {
		"_id": StringType(),
		"user_id": IntegerType(),
		"recommended_products": ArrayType(IntegerType()),
		"last_updated": TimestampType()
	},
	"ModerationQueue": {
		"_id": StringType(),
		"review_id": StringType(),
		"user_id": IntegerType(),
		"product_id": IntegerType(),
		"review_text": StringType(),
		"rating": IntegerType(),
		"moderation_status": StringType(),
		"flags": ArrayType(StringType()),
		"submitted_at": TimestampType()
	},
	"SearchQueries": {
		"_id": StringType(),
		"query_id": StringType(),
		"user_id": IntegerType(),
		"query_text": StringType(),
		"timestamp": TimestampType(),
		"filters": ArrayType(StringType()),
		"results_count": IntegerType()
	}
}

def create_spark_session() -> SparkSession:
	spark = SparkSession \
		.builder \
		.appName("ETL_final") \
		.master("local[*]") \
		.config("spark.jars", "/opt/jars/postgresql-42.7.5.jar") \
		.config("spark.mongodb.input.uri", "mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL") \
		.config("spark.mongodb.output.uri", "mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL") \
		.config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
		.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
		.getOrCreate()
	return spark

def connect_to_mongo() -> pymongo.MongoClient:
	client = MongoClient("mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL")
	db = client['ETL_MONGO_FINAL']
	return db

def collect_mongo_collections(db: pymongo.MongoClient) -> list[str]:
	return db.list_collection_names()

def extract(collection_name: str, spark: SparkSession):
	df = spark.read.format("mongo") \
		.option("uri", f"mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL.{collection_name}") \
		.load()
	df.printSchema()
	df.show(5)
	return df

def transform(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
	df = df.dropna().dropDuplicates()
	if "start_time" in df.columns:
		df = df.withColumn("start_time", F.col("start_time").cast(TimestampType()))
	if "end_time" in df.columns:
		df = df.withColumn("end_time", F.col("end_time").cast(TimestampType()))
	if "_id" in df.columns:
		df = df.withColumn("_id", F.col("_id.oid"))  # Извлекаем oid как строку
	for column in ["price_changes", "messages", "recommended_products", "flags", "filters"]:
		if column in df.columns:
			df = df.withColumn(column, F.to_json(F.col(column)))

	return df

def cast_types(df, table_name):
	schema = TABLE_SCHEMAS.get(table_name, {})
	for column, spark_type in schema.items():
		if column in df.columns:
			df = df.withColumn(column, F.col(column).cast(spark_type))
	return df

def load(df: pyspark.sql.DataFrame, table_name: str):
	logger.info(f"Schema before loading to PostgreSQL: {df.schema}")
	df.show(5)
	df.write.format("jdbc") \
		.option('driver', 'org.postgresql.Driver') \
		.option("url", 'jdbc:postgresql://postgresql/etl_data') \
		.option("dbtable", table_name) \
		.option("user", 'postgres_user') \
		.option("password", 'postgres_password') \
		.option("batchsize", 1000) \
		.mode('overwrite') \
		.save()

def main():
	spark = create_spark_session()
	db = connect_to_mongo()
	collections = collect_mongo_collections(db)
	logger.info(f'Collected collections {collections}')
	for collection in collections:
		df = extract(collection, spark)
		logger.info(f'Collected {df.count()} rows from {collection} collection')
		df = transform(df)
		logger.info(f'Transformed df contains {df.count()} rows')
		load(df, collection)

with DAG(
		'etl',
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
	etl = PythonOperator(
		task_id='etl',
		python_callable=main,
	)