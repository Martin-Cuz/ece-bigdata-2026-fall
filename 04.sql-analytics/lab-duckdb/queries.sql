-- Hello
SELECT 'Hello DuckDB' AS greeting, 21 * 2 AS answer;

-- Query the bronze layer
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bucket') || '/bronze/users.csv', header = true, strict_mode = false)
LIMIT 3;

SELECT Delimiter, Quote, HasHeader FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv', strict_mode = false);

DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false);

SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false));

-- Load the tables
CREATE OR REPLACE TABLE users AS FROM read_csv(getvariable('bucket') || '/bronze/users.csv', strict_mode = false);
CREATE OR REPLACE TABLE orders AS FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false);

SELECT (SELECT count(*) FROM users) AS users, (SELECT count(*) FROM orders) AS orders;
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;

-- Data quality checks
SELECT count(*) AS orphan_orders FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
SELECT count(*) AS inactive_users FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
SELECT uuid, count(*) AS occurrences FROM orders GROUP BY uuid HAVING count(*) > 1;

-- Aggregations
SELECT product, count(*) AS orders, sum(quantity) AS quantity,
  round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders GROUP BY product ORDER BY quantity DESC;

SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders GROUP BY month ORDER BY month;

-- Joins
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY quantity DESC LIMIT 5;

SELECT (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
  count(DISTINCT u.uuid) AS users, count(*) AS orders, round(avg(o.quantity), 2) AS avg_quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group ORDER BY age_group;

-- Window functions
WITH daily AS (
  SELECT date::DATE AS day, sum(quantity) AS quantity FROM orders GROUP BY day
)
SELECT day, quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily ORDER BY day LIMIT 10;

SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;

-- Pivot
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product USING sum(quantity) ORDER BY month;

-- Exercise 1
SELECT round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- Exercise 2
SELECT u.username, min(o.date) AS first_order, max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY days_between DESC LIMIT 5;

-- Exercise 3
SELECT hour(date) AS hour_of_day, sum(quantity) AS quantity
FROM orders GROUP BY hour_of_day ORDER BY quantity DESC LIMIT 3;

-- Exercise 4
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders GROUP BY ALL
)
SELECT product, strftime(month, '%Y-%m') AS month, quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- Exercise 5
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;

-- Parquet export
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);

SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');

-- Hive partitioning
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product), OVERWRITE_OR_IGNORE);
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');

EXPLAIN ANALYZE
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
