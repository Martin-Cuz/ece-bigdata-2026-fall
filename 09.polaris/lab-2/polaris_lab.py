import os
import sys

from pyspark.sql import SparkSession

MODE = sys.argv[1]
PRINCIPAL = {"analyst": "analyst", "reader": "reader"}.get(MODE, "dev")
CREDENTIAL = os.environ[f"POLARIS_{PRINCIPAL.upper()}_CREDENTIAL"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder.appName("lab-polaris")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")


def section(title):
    print(f"\n### {title}", flush=True)


def sql(query, n=20):
    spark.sql(query).show(n, truncate=False)


def attempt(label, query):
    try:
        rows = spark.sql(query).collect()
        print(label, "-> succeeded", [list(r) for r in rows][:6])
    except Exception as e:
        message = str(e).replace("\n", " ")
        print(label, "-> FAILED:", type(e).__name__, message[:260])


section(f"principal {PRINCIPAL}, mode {MODE}")
print("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))

if MODE == "prep":
    section("prepare the hadoop catalog tables")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").writeTo("lab.silver.orders").createOrReplace()
    spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet").writeTo("lab.silver.users").createOrReplace()
    sql("SELECT count(*) AS orders FROM lab.silver.orders")

elif MODE == "dev1":
    section("two catalogs")
    sql("SHOW NAMESPACES IN lab")
    sql("SHOW NAMESPACES IN polaris")

    section("register silver.orders")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    sql(
        f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => '{os.environ['METADATA_FILE']}'
        )
        """
    )
    sql("SELECT count(*) AS orders FROM polaris.silver.orders")

    section("tables and snapshots as dev")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")

    section("create silver.users through polaris")
    spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
    sql("SELECT count(*) AS users FROM polaris.silver.users")

    section("schema change as dev")
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    sql("DESCRIBE polaris.silver.users")
    sql("SELECT file FROM polaris.silver.users.metadata_log_entries ORDER BY timestamp")

elif MODE == "analyst":
    section("read as the analyst")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product ORDER BY product")
    attempt("INSERT as analyst", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    attempt("ALTER TABLE as analyst", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

elif MODE == "dev2":
    section("snapshots of silver.orders after the failed insert")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
    sql("SELECT count(*) AS orders FROM polaris.silver.orders")

    section("exercise 1: drop the country column")
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots")
    spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    sql("DESCRIBE polaris.silver.users")
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots")
    sql("SELECT count(*) AS metadata_files FROM polaris.silver.users.metadata_log_entries")

    section("exercise 2: gold namespace and table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE polaris.gold.product_summary AS
        SELECT product, count(*) AS orders, sum(quantity) AS quantity
        FROM polaris.silver.orders GROUP BY product
        """
    )
    sql("SHOW NAMESPACES IN polaris")
    sql("SELECT count(*) AS gold_rows FROM polaris.gold.product_summary")

elif MODE == "gold":
    section("gold namespace and table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE polaris.gold.product_summary AS
        SELECT product, count(*) AS orders, sum(quantity) AS quantity
        FROM polaris.silver.orders GROUP BY product
        """
    )
    sql("SELECT count(*) AS gold_rows FROM polaris.gold.product_summary")

elif MODE == "reader":
    section("read as the silver_reader principal")
    attempt("list namespaces of the catalog", "SHOW NAMESPACES IN polaris")
    attempt("list silver tables", "SHOW TABLES IN polaris.silver")
    attempt("read silver.orders", "SELECT count(*) FROM polaris.silver.orders")
    attempt("list gold tables", "SHOW TABLES IN polaris.gold")
    attempt("read gold.product_summary", "SELECT count(*) FROM polaris.gold.product_summary")
    attempt("insert into silver.orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")

elif MODE == "cleanup":
    section("cleanup as dev")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.product_summary")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    sql("SHOW NAMESPACES IN polaris")
    sql("SHOW TABLES IN polaris.silver")

spark.stop()
print("\n### done")
