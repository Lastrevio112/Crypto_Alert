import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

# 1. Read the raw table
df = spark.table("crypto_project.raw_prices_with_ts")

# 2. Transform dim coin
dim = (
    df.select("name")
      .distinct()
      .orderBy("name")
      .withColumn("coin_id",  F.row_number().over(Window.orderBy("name")))
      .withColumnRenamed("name", "coin_desc")
      .select("coin_id", "coin_desc")
)

# 3. Overwrite to S3
(dim.write
    .mode("overwrite")
    .parquet("s3://crypto-project-star/d_coin/")
)

job.commit()
