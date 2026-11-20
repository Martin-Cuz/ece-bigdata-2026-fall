D="$(cd "$(dirname "$0")" && pwd)"
cd "$D/../../ece-2026-bigdata"
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
echo "### jars"
ls $SPARK_HOME/jars | grep -i -E 'hadoop-aws|bundle' | head
spark-submit "$D/silver_lab.py" > ~/silver_spark.log 2>&1
grep -v -E '^(WARNING|Setting default log|To adjust logging|Using Spark|[0-9]{2}/[0-9]{2}/[0-9]{2} )|^:: |^downloading|^Ivy|^The jars' ~/silver_spark.log
echo "### s3 listings"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/silver/" --recursive
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/warehouse/" --recursive | head -40
echo "### dbt parquet for the comparison"
cd lab_medallion
export PATH="$HOME/.local/bin:$PATH"
uv sync -q
rm -f lab_medallion.duckdb
uv run dbt build > /dev/null 2>&1
dq() { uv run python -c "import duckdb,sys; c=duckdb.connect('lab_medallion.duckdb'); [print(r) for r in c.execute(sys.argv[1]).fetchall()]" "$1"; }
dq "COPY (SELECT * FROM main_silver.stg_users) TO 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet' (FORMAT parquet)"
dq "COPY (SELECT * FROM main_silver.stg_orders) TO 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet' (FORMAT parquet)"
echo "### compare with dbt"
dq "SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet' UNION ALL SELECT 'spark', count(*), sum(quantity) FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'"
dq "SELECT 'dbt' AS engine, count(*) AS users FROM 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet' UNION ALL SELECT 'spark', count(*) FROM 's3://$LAB_BUCKET_NAME/silver/spark/users.parquet/*.parquet'"
dq "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'"
dq "DESCRIBE SELECT * FROM 's3://$LAB_BUCKET_NAME/silver/spark/orders.parquet/*.parquet'"
rm -f lab_medallion.duckdb
echo "### done"
