from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TaxiTrips") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2024-01.parquet")

df.show()
df.printSchema()

spark.stop()
