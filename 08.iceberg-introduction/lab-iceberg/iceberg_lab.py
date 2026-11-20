import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder.appName("lab-iceberg")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def section(title):
    print(f"\n### {title}", flush=True)


def sql(query, truncate=False, n=20):
    spark.sql(query).show(n, truncate=truncate)


section("catalog")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
sql("SHOW NAMESPACES IN lab")

section("create the orders table")
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
stg_orders.writeTo("lab.silver.orders").createOrReplace()
sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders")

section("snapshots and files")
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots")
sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files")

section("second snapshot")
orders = spark.table("lab.silver.orders")
orders.limit(3).withColumn("order_id", F.expr("uuid()")).writeTo("lab.silver.orders").append()
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at")
sql("SELECT count(*) AS orders FROM lab.silver.orders")
sql("SELECT count(*) AS data_files FROM lab.silver.orders.files")

section("time travel")
snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
sql(f"SELECT count(*) AS orders_at_first_snapshot FROM lab.silver.orders VERSION AS OF {first_snapshot}")
sql("SELECT count(*) AS orders_now FROM lab.silver.orders")

section("schema evolution")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
sql("DESCRIBE lab.silver.orders")
spark.sql(
    """
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
    """
)
sql("SELECT source, count(*) AS orders FROM lab.silver.orders GROUP BY source ORDER BY source")
sql(f"SELECT count(*) AS orders_first_snapshot_has_source FROM lab.silver.orders VERSION AS OF {first_snapshot} WHERE source IS NOT NULL")
sql("SELECT snapshot_id, operation FROM lab.silver.orders.snapshots ORDER BY committed_at")

section("exercise 1 to 3: users table, append, time travel")
stg_users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_users.writeTo("lab.silver.users").createOrReplace()
first_users_snapshot = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at LIMIT 1").first()[0]
new_users = stg_users.limit(2).withColumn("user_id", F.expr("uuid()")).withColumn("username", F.concat(F.col("username"), F.lit("_new")))
new_users.writeTo("lab.silver.users").append()
sql("SELECT snapshot_id, operation, summary['added-records'] AS added_records FROM lab.silver.users.snapshots ORDER BY committed_at")
sql(f"SELECT count(*) AS users_before FROM lab.silver.users VERSION AS OF {first_users_snapshot}")
sql("SELECT count(*) AS users_after FROM lab.silver.users")

section("exercise 4: country column")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql(
    """
    INSERT INTO lab.silver.users
    SELECT uuid(), username || '_fr', name, sex, email, birthdate, street, city, state, zip_code, 'FR'
    FROM lab.silver.users LIMIT 1
    """
)
sql("SELECT country, count(*) AS users FROM lab.silver.users GROUP BY country ORDER BY country")

section("exercise 5: layout of the tables")
spark.stop()
print("\n### done")
