from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

def main():
	spark = SparkSession.builder.appName('etl_hw_13').getOrCreate()
	
	df = spark.createDataFrame([
		Row(msg='I hate niggers'),
		Row(msg='I am a nigger')
	])
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
	.save()

if __name__ == "__main__":
	main()
