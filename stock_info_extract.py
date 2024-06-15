import os.path

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, first, last, sum as sum_, col, from_unixtime, date_format
from pyspark.sql.window import Window


input_base_dir = "/opt/spark/stock_data"
output_base_dir = "/opt/spark/stock_result/outputs"

spark = SparkSession.builder.appName(
    "read_parquet_files"
).getOrCreate()

parquet_dir_path = os.path.join(input_base_dir, "partitioned")
df = spark.read.parquet(parquet_dir_path)

df = df.select(["ts_event", "price", "size"])

df = df.withColumn('ts', (df['ts_event']/1000000000).cast("long"))
df = df.withColumn('new_price', (df['price']/10000000).cast("int"))
df = df.drop('price')
df = df.withColumnRenamed('new_price', 'price')
windowSpec = Window.partitionBy("ts").orderBy("ts_event")\
.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_with_open_close = df.withColumn("open", first("price").over(windowSpec))
df_with_open_close = df_with_open_close.withColumn("close", last("price").over(windowSpec))
result = df_with_open_close.groupBy("ts")\
.agg(
    first("open").alias("open"),
    last("close").alias("close"),
    max("price").alias("high"),
    min("price").alias("low"),
    sum_("size").alias("volume")
).orderBy("ts")

result = result.withColumn("year", date_format(from_unixtime(col("ts")), "yyyy"))\
               .withColumn("month", date_format(from_unixtime(col("ts")), "MM"))\
               .withColumn("day", date_format(from_unixtime(col("ts")), "dd"))

result.show(10)

result.write.partitionBy("year", "month", "day").parquet(output_base_dir)

