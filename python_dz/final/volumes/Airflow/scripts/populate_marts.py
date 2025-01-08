import argparse
from pyspark.sql import SparkSession


def create_user_activity_view(src_tgt_url: str, src_tgt_driver: str, tgt_mart_name: str):
	# Создание Spark сессии
	spark = (
		SparkSession.builder
		.appName(f"Create_mart_{tgt_mart_name}")
		.getOrCreate()
	)

	users_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "users") \
		.load()
	)

	orders_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "orders")
		.load()
	)

	user_activity_df = (
		orders_df.join(users_df, "user_id")
		.groupBy("user_id", "first_name", "last_name", "status")
		.agg({"order_id": "count", "total_amount": "sum"})
		.withColumnRenamed("count(order_id)", "order_count")
		.withColumnRenamed("sum(total_amount)", "total_spent")
	)

	(
		user_activity_df.write
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", f"mart_{tgt_mart_name}")
		.mode("overwrite")
		.save()
	)

	spark.stop()


def create_product_sales_view(src_tgt_url: str, src_tgt_driver: str, tgt_mart_name: str):
	spark = (
		SparkSession.builder
		.appName(f"Create_mart_{tgt_mart_name}")
		.getOrCreate()
	)

	products_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "products") \
		.load()
	)

	order_details_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "order_details") \
		.load()
	)

	orders_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "orders") \
		.load()
	)

	product_sales_df = (
		order_details_df.join(products_df, "product_id")
		.join(orders_df, "order_id")
		.groupBy("product_id", "name", "status")
		.agg({"quantity": "sum", "total_price": "sum"})
		.withColumnRenamed("sum(quantity)", "total_quantity_sold")
		.withColumnRenamed("sum(total_price)", "total_sales")
	)

	(
		product_sales_df.write
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", f"mart_{tgt_mart_name}")
		.mode("overwrite")
		.save()
	)

	spark.stop()


def create_average_check_view(src_tgt_url: str, src_tgt_driver: str, tgt_mart_name: str):
	spark = (
		SparkSession.builder
		.appName(f"Create_mart_{tgt_mart_name}")
		.getOrCreate()
	)

	orders_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "orders") \
		.load()
	)

	users_df = (
		spark.read
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", "users") \
		.load()
	)

	average_check_df = (
		orders_df.join(users_df, "user_id")
		.groupBy("status", "loyalty_status")
		.agg({"total_amount": "avg"})
		.withColumnRenamed("avg(total_amount)", "average_check")
	)

	(
		average_check_df.write
		.format("jdbc")
		.option("url", src_tgt_url)
		.option("driver", src_tgt_driver)
		.option("dbtable", f"mart_{tgt_mart_name}")
		.mode("overwrite")
		.save()
	)

	spark.stop()


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--src_tgt_url', type=str, required=True)
	parser.add_argument("--src_tgt_driver", type=str, required=True)
	parser.add_argument('--target_mart', type=str, required=True, choices=['user_activity', 'product_sales', 'average_check'])
	args = parser.parse_args()

	if args.target_mart == "user_activity":
		create_user_activity_view(args.src_tgt_url, args.src_tgt_driver, args.target_mart)
	elif args.target_mart == "product_sales":
		create_product_sales_view(args.src_tgt_url, args.src_tgt_driver, args.target_mart)
	elif args.target_mart == "average_check":
		create_average_check_view(args.src_tgt_url, args.src_tgt_driver, args.target_mart)


if __name__ == "__main__":
	main()