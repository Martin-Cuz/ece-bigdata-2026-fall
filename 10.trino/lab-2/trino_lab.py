import os
import time

import pandas as pd
import trino

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 70)

CATALOG = "lakehouse"

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"],
    port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"],
    http_scheme="http",
    catalog=CATALOG,
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(label, statement, n=None):
    print(f"\n-- {label}", flush=True)
    try:
        df = sql(statement)
        print(df.to_string(index=False) if n is None else df.head(n).to_string(index=False), flush=True)
        return df
    except Exception as e:
        print("FAILED:", type(e).__name__, str(e).replace("\n", " ")[:300], flush=True)
        return None


def section(title):
    print(f"\n### {title}", flush=True)


section("connection")
show("version", "SELECT version()")

section("catalogs, schemas and tables")
show("SHOW CATALOGS", "SHOW CATALOGS")
show("SHOW SCHEMAS", "SHOW SCHEMAS FROM lakehouse")
show("SHOW TABLES silver", "SHOW TABLES FROM lakehouse.silver")
show("DESCRIBE orders", "DESCRIBE lakehouse.silver.orders")
show("DESCRIBE users", "DESCRIBE lakehouse.silver.users")

section("query the silver layer")
show("orders", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
show("users", "SELECT count(*) AS users FROM lakehouse.silver.users")
show(
    "address parsing",
    """
    SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users GROUP BY state ORDER BY users DESC, state LIMIT 10
    """,
)
show(
    "orphan orders",
    """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
    """,
)

section("iceberg metadata")
show("snapshots", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at')
show("files", 'SELECT regexp_extract(file_path, \'[^/]+$\') AS file, file_format, record_count, file_size_in_bytes FROM lakehouse.silver."orders$files"')
show("history", 'SELECT * FROM lakehouse.silver."orders$history"')

section("query plan")
plan = sql(
    """
    EXPLAIN (TYPE DISTRIBUTED)
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
    """
)
text = plan.iloc[0, 0]
for line in text.splitlines():
    stripped = line.strip()
    if stripped.startswith(("Fragment", "Output layout")) or any(
        k in stripped for k in ("TableScan", "RemoteSource", "Join[", "Aggregate", "Distribution:", "distribution", "Exchange")
    ):
        print(line[:160])
section("explain analyze")
stats = sql(
    """
    EXPLAIN ANALYZE
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
    """
)
for line in stats.iloc[0, 0].splitlines():
    stripped = line.strip()
    if stripped.startswith(("Fragment", "Query ")) or any(k in stripped for k in ("Input:", "CPU:", "Peak Memory", "Output:", "ScanFilter", "TableScan")):
        print(line[:170])
show(
    "history of queries",
    """
    SELECT state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms, substr(query, 1, 60) AS query
    FROM system.runtime.queries ORDER BY created DESC LIMIT 5
    """,
)

section("gold layer")
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
for table in ["orders_by_state_month", "top_products", "user_activity"]:
    sql(f"DROP TABLE IF EXISTS lakehouse.gold.{table}")
show("schemas", "SHOW SCHEMAS FROM lakehouse")
show(
    "CTAS orders_by_state_month",
    """
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT u.state, date_trunc('month', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
    """,
)
show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state", 6)
show(
    "CTAS top_products",
    """
    CREATE TABLE lakehouse.gold.top_products AS
    SELECT product, count(*) AS orders, sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders GROUP BY product
    """,
)
show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank")
show(
    "CTAS user_activity",
    """
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT u.user_id, u.state, date_diff('year', u.birthdate, current_date) AS age,
           count(o.order_id) AS orders, coalesce(sum(o.quantity), 0) AS units,
           min(o.ordered_at) AS first_order_at, max(o.ordered_at) AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
    """,
)
show("user_activity", "SELECT user_id, state, age, orders, units FROM lakehouse.gold.user_activity ORDER BY orders DESC", 5)
show("SHOW TABLES gold", "SHOW TABLES FROM lakehouse.gold")
show(
    "files of top_products",
    'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"',
)

section("validation of the gold tables")
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
assert silver_units == gold_units == product_units, (silver_units, gold_units, product_units)
print("units: silver", silver_units, "gold", gold_units, "top_products", product_units, "-> OK")
silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
assert silver_users == gold_users and silver_orders == gold_orders
print("users:", silver_users, gold_users, "orders:", silver_orders, gold_orders, "-> OK")

section("refresh a gold table")
sql("DELETE FROM lakehouse.gold.top_products")
show("rows after DELETE", "SELECT count(*) AS products FROM lakehouse.gold.top_products")
sql(
    """
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders GROUP BY product
    """
)
show("snapshots of top_products", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
first = sql('SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at LIMIT 1').iloc[0, 0]
show("time travel", f"SELECT count(*) AS products_at_creation FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
show("current", "SELECT count(*) AS products_now FROM lakehouse.gold.top_products")

section("star schema: dimensions")
for table in ["fact_orders", "dim_user", "dim_product", "dim_date"]:
    sql(f"DROP TABLE IF EXISTS lakehouse.gold.{table}")
sql(
    """
    CREATE TABLE lakehouse.gold.dim_user AS
    SELECT row_number() OVER (ORDER BY user_id) AS user_key,
           user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
    """
)
sql(
    """
    CREATE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
    """
)
first_day, last_day = sql(
    """
    SELECT CAST(min(ordered_at) AS date) AS first_day, CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
    """
).iloc[0]
print("date range:", first_day, last_day)
sql(
    f"""
    CREATE TABLE lakehouse.gold.dim_date AS
    SELECT CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
           d AS full_date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
           date_format(CAST(d AS timestamp), '%M') AS month_name,
           day_of_week(d) AS day_of_week, day_of_week(d) IN (6, 7) AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
    """
)
section("star schema: fact table")
sql(
    """
    CREATE TABLE lakehouse.gold.fact_orders AS
    SELECT o.order_id, u.user_key, p.product_key,
           CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
           o.ordered_at, o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
    """
)
show(
    "row counts",
    """
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
    """,
)
show("dim_date sample", "SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key", 4)

section("query the star")
show(
    "top 10 year/state/product",
    """
    SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product ORDER BY units DESC, u.state, p.product LIMIT 10
    """,
)
star = show(
    "orders_by_state_month rebuilt from the star",
    """
    SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units, count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month ORDER BY d.year, d.month, u.state LIMIT 6
    """,
)
show(
    "same rows in orders_by_state_month",
    "SELECT state, year(month) AS year, month(month) AS month, orders, units, buyers FROM lakehouse.gold.orders_by_state_month ORDER BY month, state",
    6,
)

section("validation of the star")
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"
unmatched = sql(
    """
    SELECT count(*) AS v FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
    """
).iloc[0, 0]
assert unmatched == 0, unmatched
print("fact rows", fact_orders, "units", fact_units, "unmatched", unmatched, "-> OK")

section("exercise 1: partitioned table")
sql("DROP TABLE IF EXISTS lakehouse.gold.orders_by_state_month_part")
sql(
    """
    CREATE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)'])
    AS SELECT * FROM lakehouse.gold.orders_by_state_month
    """
)
show("files unpartitioned", 'SELECT count(*) AS files, sum(record_count) AS records FROM lakehouse.gold."orders_by_state_month$files"')
show("files partitioned", 'SELECT count(*) AS files, sum(record_count) AS records FROM lakehouse.gold."orders_by_state_month_part$files"')
show("partitions", 'SELECT partition, record_count, file_count FROM lakehouse.gold."orders_by_state_month_part$partitions" ORDER BY 1')
for name in ["orders_by_state_month", "orders_by_state_month_part"]:
    stats = sql(
        f"""
        EXPLAIN ANALYZE
        SELECT state, sum(units) AS units FROM lakehouse.gold.{name}
        WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC' GROUP BY state
        """
    )
    print(f"\n-- EXPLAIN ANALYZE on {name}")
    for line in stats.iloc[0, 0].splitlines():
        if "Input:" in line and "ScanFilter" not in line or "TableScan" in line or "ScanFilter" in line:
            print(line.strip()[:170])
show(
    "units in February",
    "SELECT sum(units) AS units FROM lakehouse.gold.orders_by_state_month_part WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'",
)

section("exercise 2: cohorts")
sql("DROP TABLE IF EXISTS lakehouse.gold.cohorts")
sql(
    """
    CREATE TABLE lakehouse.gold.cohorts AS
    SELECT (year(u.birthdate) / 10) * 10 AS decade,
           count(DISTINCT u.user_id) AS users,
           count(o.order_id) AS orders,
           round(CAST(sum(o.quantity) AS double) / nullif(count(o.order_id), 0), 2) AS avg_units_per_order
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY 1
    """
)
show("cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY decade")
cohort_users = sql("SELECT sum(users) AS v FROM lakehouse.gold.cohorts").iloc[0, 0]
cohort_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.cohorts").iloc[0, 0]
assert cohort_users == silver_users and cohort_orders == silver_orders
print("cohorts users", cohort_users, "orders", cohort_orders, "-> OK")

section("exercise 3: insert, merge, time travel")
gold_first = sql('SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at LIMIT 1').iloc[0, 0]
sql(
    """
    INSERT INTO lakehouse.silver.orders
    SELECT 'trino-lab-order-0001', user_id, ordered_at, 2, 'bread' FROM lakehouse.silver.orders LIMIT 1
    """
)
show("silver orders after the insert", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
merge = """
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state, date_trunc('month', o.ordered_at) AS month,
               count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders AS o JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s ON t.state = s.state AND t.month = s.month
    WHEN MATCHED THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)
"""
sql(merge)
show("gold units after the merge", "SELECT sum(units) AS units, sum(orders) AS orders FROM lakehouse.gold.orders_by_state_month")
show(
    "gold units at creation (FOR VERSION AS OF)",
    f"SELECT sum(units) AS units, sum(orders) AS orders FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {gold_first}",
)
show("snapshots of orders_by_state_month", 'SELECT snapshot_id, operation FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at')
sql("DELETE FROM lakehouse.silver.orders WHERE order_id = 'trino-lab-order-0001'")
sql(merge)
show("silver restored", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
show("gold restored", "SELECT sum(units) AS units, sum(orders) AS orders FROM lakehouse.gold.orders_by_state_month")

section("exercise 4: optimize")
show("snapshots before", 'SELECT count(*) AS snapshots FROM lakehouse.gold."top_products$snapshots"')
show("files before", 'SELECT count(*) AS files, sum(record_count) AS records FROM lakehouse.gold."top_products$files"')
sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
show("snapshots after", 'SELECT snapshot_id, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
show("files after", 'SELECT count(*) AS files, sum(record_count) AS records FROM lakehouse.gold."top_products$files"')

section("exercise 5: numbers to compare with spark")
show("orders_by_state_month", "SELECT count(*) AS row_count, sum(orders) AS orders, sum(units) AS units FROM lakehouse.gold.orders_by_state_month")

section("final tables")
show("gold tables", "SHOW TABLES FROM lakehouse.gold")
conn.close()
print("\n### done")
