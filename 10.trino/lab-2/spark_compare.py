import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CREDENTIAL = os.environ["POLARIS_DEV_CREDENTIAL"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder.appName("lab-trino-compare")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{os.environ['POLARIS_URI']}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential", CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")

orders = spark.table("polaris.silver.orders")
users = spark.table("polaris.silver.users")
result = (
    orders.join(users, "user_id")
    .groupBy("state", F.date_trunc("month", "ordered_at").alias("month"))
    .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"), F.countDistinct("user_id").alias("buyers"))
)
print("\n### spark aggregation")
totals = result.agg(F.count("*").alias("row_count"), F.sum("orders").alias("orders"), F.sum("units").alias("units")).collect()[0]
print("spark orders_by_state_month: row_count", totals["row_count"], "orders", totals["orders"], "units", totals["units"])
gold = spark.table("polaris.gold.orders_by_state_month")
gtotals = gold.agg(F.count("*").alias("row_count"), F.sum("orders").alias("orders"), F.sum("units").alias("units")).collect()[0]
print("gold table read by spark:   row_count", gtotals["row_count"], "orders", gtotals["orders"], "units", gtotals["units"])
spark.sql("SHOW TABLES IN polaris.gold").show(truncate=False)
spark.stop()
print("\n### done")
