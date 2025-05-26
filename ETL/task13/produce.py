
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct


def main():
	spark = SparkSession.builder.appName('etl_hw_13').getOrCreate()

	query = spark.readStream.format('kafka') \
	.option("kafka.bootstrap.servers", "rc1b-tn81ud29f4nbste0.mdb.yandexcloud.net:9691") \
	.option("subscribe", "Niggers") \
	.option("kafka.security.protocol", "SASL_SSL") \
	.option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
	.option("kafka.sasl.jaas.config",
	"org.apache.kafka.common.security.scram.ScramLoginModule required "
	"username=user1 "
	"password=passwordl "
	";") \
	.option("startingOffsets", "earliest")\
	.load (\
	.selectExpr("CAST(value AS STRING)")\
	.where(col("value") -isNotNull))\
	.writeStream\
	.trigger(once=True) \
	.queryName("received_messages") \
	.format("memory") \
	.start()

	query.awaitTermination()

	df = spark.sql("select value from recieved_messages")

	df.write.format("text").save("https://storage.yandexcloud.net/etl.hw/out")

if __name__ == "__main__":
	main()
