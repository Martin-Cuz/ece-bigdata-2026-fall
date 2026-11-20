D="$(cd "$(dirname "$0")" && pwd)"
cd "$D/../../ece-2026-bigdata"
export PATH="$HOME/.local/bin:$PATH"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile default)
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
B="s3://$LAB_BUCKET_NAME"
echo "### version"; duckdb --version
echo "### secrets"
duckdb -c "SELECT name, type, provider, persistent, storage FROM duckdb_secrets();"
echo "### regenerate secret"
duckdb -c "
  DROP PERSISTENT SECRET IF EXISTS s3_onyxia_connection;
  CREATE PERSISTENT SECRET s3_onyxia_connection (
    TYPE s3,
    PROVIDER credential_chain,
    ENDPOINT 's3.seaweedfs.adm.adaltas.cloud',
    REGION 'us-east-1',
    URL_STYLE 'path');"
cat > init.sql <<SQL
SET VARIABLE bucket = '$B';
SET TimeZone = 'UTC';
SQL
echo "### queries"
rm -f analytics.duckdb
duckdb analytics.duckdb -init init.sql -markdown -c ".read $D/queries.sql" 2>&1
echo "### large dataset"
uv run dataset-orders -u 5000 -o csv > orders_large.csv
ls -lh orders_large.csv
s5cmd --endpoint-url "$S3_ENDPOINT_URL" --profile default cp orders_large.csv "$B/large/orders.csv"
duckdb analytics.duckdb -init init.sql -c "
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false))
TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');"
aws s3 --profile default ls "$B/large/"
for query in \
  "FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false) SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT count(*) WHERE date >= '2100-01-01'"
do
  echo "$query"
  duckdb -init init.sql -c "SET enable_external_file_cache = false; EXPLAIN ANALYZE $query" 2>/dev/null \
  | grep -E 'in:|#GET|Total Time'
done
duckdb -init init.sql -c "
  SELECT row_group_id, row_group_num_rows, stats_min, stats_max
  FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet')
  WHERE path_in_schema = 'date'"
echo "### python api"
uv run orders-report
uv run orders-report -p cookie
echo "### cleanup"
aws s3 --profile default rm "$B/analytics/" --recursive --only-show-errors
aws s3 --profile default rm "$B/large/" --recursive --only-show-errors
rm -f orders_large.csv
aws s3 --profile default ls "$B/" --recursive
echo "### done"
