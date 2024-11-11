import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date, datetime, timedelta, date_format, concat, year, lpad, weekofyear
from pyspark import SparkConf

# Initialize Spark configuration
conf = SparkConf() \
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .set("spark.sql.orc.enabled", "true") \
    .set("spark.dynamicAllocation.enabled", "true") \
    .set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
    .set("spark.hadoop.fs.s3a.committer.name", "directory") \
    .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Create Spark context and Glue context
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Extract
df_path = 's3a://your_bucket_name/folder/input_table/'
df = spark.read.format("parquet").load(df_path) # chane format if needed

# Transform
# example of partitioning a table
df = df.withColumn("partition_id", concat(
    year("transaction_col"),  #example
    lpad(weekofyear("transaction_col").cast("string"), 2, "0")
))

# Load
df_target_path = 's3a://your_bucket_name/folder/target_table/'
final_df = df.repartition('partition_id') # write one file per partition

final_df.write \
    .format('parquet') \
    .mode('overwrite') \
    .partitionBy('partition_id') \
    .save(df_target_path)
