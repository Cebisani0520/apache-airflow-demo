import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("etl_transform").getOrCreate()


# Input path (mounted from host via docker-compose)
input_path = "/opt/airflow/data/raw_sample.csv"
output_path = "/opt/spark-apps/output/transformed.parquet"


# read CSV
df = spark.read.option("header", True).csv(input_path)


# basic cleaning / cast
df2 = df.withColumn(
    "signup_date", to_date(col("signup_date"), "yyyy-MM-dd")
).withColumn("score", col("score").cast("double"))


# add a derived column (for demo)
from pyspark.sql.functions import when

df3 = df2.withColumn("quality", when(col("score") >= 15, "high").otherwise("low"))


# write transformed data
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df3.write.mode("overwrite").parquet(output_path)


print("Wrote transformed data to", output_path)


spark.stop()
