import os
import re
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder.appName("lab-silver-pyspark")
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


section("versions")
print("spark", spark.version)

section("bronze")
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
users_bronze.printSchema()
users_bronze.select("address").show(3, truncate=False)
print("bronze users/orders:", users_bronze.count(), orders_bronze.count())

section("silver users (dataframe api)")
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

section("silver orders with duplicates")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
orders_with_duplicates = orders_typed.unionByName(orders_typed.limit(3))
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates.withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

section("spark sql")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql(
    """
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
    """
)
print("sql dedup count:", stg_orders_sql.count(), "same columns:", set(stg_orders.columns) == set(stg_orders_sql.columns))
stg_users.createOrReplaceTempView("stg_users")
spark.sql(
    """
    SELECT state, count(*) AS users, sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
    """
).show(10)

section("parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("parquet users/orders:", users_silver.count(), orders_silver.count(), "partitions:", orders_silver.rdd.getNumPartitions())

section("iceberg")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
print("iceberg users/orders:", spark.table("lab.silver.users").count(), spark.table("lab.silver.orders").count())

section("exercise 1: birthdate_is_valid")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate") <= F.col("first_order_at").cast("date"), F.lit(True)),
    )
    .drop("first_order_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql(
    """
    SELECT u.*, coalesce(u.birthdate <= CAST(f.first_order_at AS DATE), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id) f
    ON u.user_id = f.user_id
    """
)
print(
    "invalid birthdates df/sql:",
    users_valid_df.filter(~F.col("birthdate_is_valid")).count(),
    users_valid_sql.filter(~F.col("birthdate_is_valid")).count(),
    "of",
    users_valid_df.count(),
)

section("exercise 2: udf vs regexp_extract")
pattern = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@F.udf("string")
def zip_udf(address):
    lines = address.split("\n")
    m = pattern.search(lines[1]) if len(lines) > 1 else None
    return m.group(2) if m else ""


def timed(label, df):
    t = time.perf_counter()
    rows = df.collect()
    print(label, f"{time.perf_counter() - t:.3f}s", len(rows), "rows")


timed(
    "regexp_extract",
    users_bronze.select(
        F.regexp_extract(F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2).alias("z")
    ),
)
timed("python udf", users_bronze.select(zip_udf("address").alias("z")))
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("string")
    def zip_pandas(addresses: pd.Series) -> pd.Series:
        return addresses.str.split("\n").str[1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

    timed("pandas_udf", users_bronze.select(zip_pandas("address").alias("z")))
except Exception as e:
    print("pandas_udf not available:", type(e).__name__, str(e)[:80])

section("exercise 3: cache")
orders_path = f"s3a://{BUCKET}/bronze/orders.csv"
for label, df in [
    ("uncached", spark.read.csv(orders_path, header=True, inferSchema=True)),
    ("cached", spark.read.csv(orders_path, header=True, inferSchema=True).cache()),
]:
    df.count()
    for i in range(3):
        t = time.perf_counter()
        df.groupBy("product").count().collect()
        print(label, f"query {i + 1}: {time.perf_counter() - t:.3f}s")

section("exercise 4: append and time travel")
first_snapshot = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at LIMIT 1").first()[0]
new_order = spark.createDataFrame(
    [("11111111-1111-1111-1111-111111111111", stg_users.first()["user_id"], "2020-05-01 00:00:00", 2, "bread")],
    ["order_id", "user_id", "ordered_at", "quantity", "product"],
).withColumn("ordered_at", F.col("ordered_at").cast("timestamp"))
new_order.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)
print(
    "now:",
    spark.sql("SELECT count(*) FROM lab.silver.orders").first()[0],
    "before the append (VERSION AS OF):",
    spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first_snapshot}").first()[0],
)

spark.stop()
print("\n### done")
