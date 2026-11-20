D="$(cd "$(dirname "$0")" && pwd)"
cd "$D/../../ece-2026-bigdata/lab_medallion"
export PATH="$HOME/.local/bin:$PATH"
export LAB_BUCKET_NAME=$KUBERNETES_NAMESPACE
echo "### install"; uv sync -q; uv run dbt --version | head -8
echo "### debug"; uv run dbt debug 2>&1 | tail -8
echo "### bronze preview"
uv run dbt show -q --limit 3 --inline "select uuid, user_uuid, date, quantity, product from {{ source('bronze', 'orders') }}"
rm -f lab_medallion.duckdb
echo "### seed"; uv run dbt seed 2>&1 | tail -6
echo "### run"; uv run dbt run --select silver 2>&1 | tail -8
echo "### users sample"
uv run dbt show -q --limit 3 --inline "select username, street, city, state, zip_code, birthdate_is_valid from {{ ref('stg_users') }}"
echo "### invalid birthdates"
uv run dbt show -q --inline "select count(*) as invalid_birthdates, (select count(*) from {{ ref('stg_users') }}) as users from {{ ref('stg_users') }} where not birthdate_is_valid"
echo "### test"; uv run dbt test 2>&1 | grep -E "PASS|WARN|FAIL|ERROR|Done" | cut -c1-140
echo "### build"; rm -f lab_medallion.duckdb; uv run dbt build 2>&1 | grep -E "Done|WARN|ERROR|FAIL" | cut -c1-140
echo "### lineage"
uv run dbt ls -q --resource-type model
uv run dbt ls -q --resource-type source model --select +stg_orders
echo "### docs"; uv run dbt docs generate --static 2>&1 | tail -2; ls -l target/static_index.html
echo "### parquet export"
duckdb lab_medallion.duckdb -c "COPY (SELECT * FROM main_silver.stg_users) TO 's3://$LAB_BUCKET_NAME/silver/dbt/users.parquet' (FORMAT parquet); COPY (SELECT * FROM main_silver.stg_orders) TO 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet' (FORMAT parquet);"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/silver/dbt/"
duckdb lab_medallion.duckdb -c "SELECT sex, count(*) AS users FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/users.parquet') GROUP BY sex ORDER BY sex;"
duckdb lab_medallion.duckdb -c "SELECT (SELECT count(*) FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet')) AS parquet_orders, (SELECT count(*) FROM main_silver.stg_orders) AS view_orders;"
echo "### materialization experiment (table)"
rm -f lab_medallion.duckdb
SILVER_MATERIALIZED=table uv run dbt build 2>&1 | grep -E "Done|table model" | cut -c1-120
ls -l lab_medallion.duckdb
duckdb -readonly lab_medallion.duckdb -c "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main_silver' ORDER BY 1;"
rm -f lab_medallion.duckdb
uv run dbt build 2>&1 | grep -E "Done" | cut -c1-120
ls -l lab_medallion.duckdb
echo "### cleanup"
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/silver/" --recursive --only-show-errors
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/" --recursive
echo "### done"
