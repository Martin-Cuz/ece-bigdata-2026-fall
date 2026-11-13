# Lab DuckDB: execution results

## Hello

```sql
SELECT 'Hello DuckDB' AS greeting, 21 * 2 AS answer;
```

| greeting | answer |
|---|---|
| Hello DuckDB | 42 |

## Query the bronze layer

```sql
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bucket') || '/bronze/users.csv', header = true, strict_mode = false)
LIMIT 3;
```

| uuid | username | name | birthdate |
|---|---|---|---|
| bdd640fb-0667-4ad1-9c80-317fa3b1799d | garzaanthony | Charles Garcia | 1940-03-03 |
| 1a2a73ed-562b-4f79-8374-59eef50bea63 | lindsay78 | Heather Cooper | 1997-04-14 |
| 3eabedcb-baa8-4dd4-88bd-64072bcfbe01 | gabriellecameron | Debra Schaefer | 2012-07-14 |

## Query

```sql
SELECT Delimiter, Quote, HasHeader FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv', strict_mode = false);
```

| Delimiter | Quote | HasHeader |
|---|---|---|
| , | " | True |

## Query

```sql
DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false);
```

| column_name | column_type | null | key | default | extra |
|---|---|---|---|---|---|
| uuid | VARCHAR | YES | None | None | None |
| user_uuid | VARCHAR | YES | None | None | None |
| date | TIMESTAMP WITH TIME ZONE | YES | None | None | None |
| quantity | BIGINT | YES | None | None | None |
| product | VARCHAR | YES | None | None | None |

## Query

```sql
SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false));
```

| column_name | column_type | approx_unique | null_percentage |
|---|---|---|---|
| uuid | VARCHAR | 2589 | 0.00 |
| user_uuid | VARCHAR | 49 | 0.00 |
| date | TIMESTAMP WITH TIME ZONE | 2391 | 0.00 |
| quantity | BIGINT | 5 | 0.00 |
| product | VARCHAR | 6 | 0.00 |

## Load the tables

```sql
CREATE OR REPLACE TABLE users AS FROM read_csv(getvariable('bucket') || '/bronze/users.csv', strict_mode = false);
```

| Count |
|---|
| 50 |

## Query

```sql
CREATE OR REPLACE TABLE orders AS FROM read_csv(getvariable('bucket') || '/bronze/orders.csv', strict_mode = false);
```

| Count |
|---|
| 2538 |

## Query

```sql
SELECT (SELECT count(*) FROM users) AS users, (SELECT count(*) FROM orders) AS orders;
```

| users | orders |
|---|---|
| 50 | 2538 |

## Query

```sql
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;
```

| first_order | last_order |
|---|---|
| 2020-01-01 00:19:27+00:00 | 2020-04-15 17:39:16+00:00 |

## Data quality checks

```sql
SELECT count(*) AS orphan_orders FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
```

| orphan_orders |
|---|
| 0 |

## Query

```sql
SELECT count(*) AS inactive_users FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
```

| inactive_users |
|---|
| 1 |

## Query

```sql
SELECT uuid, count(*) AS occurrences FROM orders GROUP BY uuid HAVING count(*) > 1;
```

| uuid | occurrences |
|---|---|

## Aggregations

```sql
SELECT product, count(*) AS orders, sum(quantity) AS quantity,
  round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders GROUP BY product ORDER BY quantity DESC;
```

| product | orders | quantity | share_pct |
|---|---|---|---|
| drink | 447 | 1352 | 17.6 |
| croissant | 435 | 1343 | 17.5 |
| donut | 435 | 1316 | 17.2 |
| bread | 419 | 1279 | 16.7 |
| brioche | 414 | 1245 | 16.2 |
| cookie | 388 | 1131 | 14.8 |

## Query

```sql
SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders GROUP BY month ORDER BY month;
```

| month | orders | quantity |
|---|---|---|
| 2020-01-01 00:00:00+00:00 | 744 | 2254 |
| 2020-02-01 00:00:00+00:00 | 696 | 2151 |
| 2020-03-01 00:00:00+00:00 | 744 | 2214 |
| 2020-04-01 00:00:00+00:00 | 354 | 1047 |

## Joins

```sql
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY quantity DESC LIMIT 5;
```

| username | name | orders | quantity |
|---|---|---|---|
| clarence34 | Diane Beck | 99 | 301 |
| pearsonamber | Shannon Pierce | 97 | 297 |
| jcervantes | Melissa Roberts | 96 | 295 |
| calebrussell | Crystal Benson | 96 | 294 |
| garzaanthony | Charles Garcia | 91 | 286 |

## Query

```sql
SELECT (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
  count(DISTINCT u.uuid) AS users, count(*) AS orders, round(avg(o.quantity), 2) AS avg_quantity
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group ORDER BY age_group;
```

| age_group | users | orders | avg_quantity |
|---|---|---|---|
| 0 | 11 | 532 | 3.06 |
| 20 | 7 | 322 | 3.06 |
| 40 | 9 | 480 | 3.04 |
| 60 | 12 | 538 | 2.97 |
| 80 | 8 | 536 | 2.98 |
| 100 | 2 | 130 | 3.06 |

## Window functions

```sql
WITH daily AS (
  SELECT date::DATE AS day, sum(quantity) AS quantity FROM orders GROUP BY day
)
SELECT day, quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily ORDER BY day LIMIT 10;
```

| day | quantity | cumulative | avg_7d |
|---|---|---|---|
| 2020-01-01 | 76 | 76 | 76.0 |
| 2020-01-02 | 72 | 148 | 74.0 |
| 2020-01-03 | 77 | 225 | 75.0 |
| 2020-01-04 | 74 | 299 | 74.8 |
| 2020-01-05 | 80 | 379 | 75.8 |
| 2020-01-06 | 75 | 454 | 75.7 |
| 2020-01-07 | 75 | 529 | 75.6 |
| 2020-01-08 | 65 | 594 | 74.0 |
| 2020-01-09 | 69 | 663 | 73.6 |
| 2020-01-10 | 74 | 737 | 73.1 |

## Query

```sql
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;
```

| month | product | quantity |
|---|---|---|
| 2020-01 | donut | 412 |
| 2020-02 | croissant | 450 |
| 2020-03 | bread | 447 |
| 2020-04 | cookie | 187 |

## Pivot

```sql
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product USING sum(quantity) ORDER BY month;
```

| month | bread | brioche | cookie | croissant | donut | drink |
|---|---|---|---|---|---|---|
| 2020-01 | 373 | 366 | 350 | 368 | 412 | 385 |
| 2020-02 | 286 | 359 | 321 | 450 | 346 | 389 |
| 2020-03 | 447 | 366 | 273 | 354 | 378 | 396 |
| 2020-04 | 173 | 154 | 187 | 171 | 180 | 182 |

## Exercise 1

```sql
SELECT round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;
```

| avg_orders_per_user | avg_quantity_per_order |
|---|---|
| 51.8 | 3.02 |

## Exercise 2

```sql
SELECT u.username, min(o.date) AS first_order, max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL ORDER BY days_between DESC LIMIT 5;
```

| username | first_order | last_order | days_between |
|---|---|---|---|
| clarence34 | 2020-02-08 14:06:42+00:00 | 2020-02-12 16:47:47+00:00 | 4 |
| jcervantes | 2020-03-04 11:24:00+00:00 | 2020-03-08 10:48:48+00:00 | 4 |
| ilewis | 2020-01-17 14:41:53+00:00 | 2020-01-21 07:52:48+00:00 | 4 |
| lindsay78 | 2020-01-04 19:21:00+00:00 | 2020-01-08 08:37:41+00:00 | 4 |
| pearsonamber | 2020-01-21 08:53:13+00:00 | 2020-01-25 08:51:22+00:00 | 4 |

## Exercise 3

```sql
SELECT hour(date) AS hour_of_day, sum(quantity) AS quantity
FROM orders GROUP BY hour_of_day ORDER BY quantity DESC LIMIT 3;
```

| hour_of_day | quantity |
|---|---|
| 19 | 350 |
| 0 | 349 |
| 20 | 332 |

## Exercise 4

```sql
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders GROUP BY ALL
)
SELECT product, strftime(month, '%Y-%m') AS month, quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;
```

| product | month | quantity | variation_pct |
|---|---|---|---|
| bread | 2020-01 | 373 | None |
| bread | 2020-02 | 286 | -23.3 |
| bread | 2020-03 | 447 | 56.3 |
| bread | 2020-04 | 173 | -61.3 |
| brioche | 2020-01 | 366 | None |
| brioche | 2020-02 | 359 | -1.9 |
| brioche | 2020-03 | 366 | 1.9 |
| brioche | 2020-04 | 154 | -57.9 |
| cookie | 2020-01 | 350 | None |
| cookie | 2020-02 | 321 | -8.3 |
| cookie | 2020-03 | 273 | -15.0 |
| cookie | 2020-04 | 187 | -31.5 |
| croissant | 2020-01 | 368 | None |
| croissant | 2020-02 | 450 | 22.3 |
| croissant | 2020-03 | 354 | -21.3 |
| croissant | 2020-04 | 171 | -51.7 |
| donut | 2020-01 | 412 | None |
| donut | 2020-02 | 346 | -16.0 |
| donut | 2020-03 | 378 | 9.2 |
| donut | 2020-04 | 180 | -52.4 |
| drink | 2020-01 | 385 | None |
| drink | 2020-02 | 389 | 1.0 |
| drink | 2020-03 | 396 | 1.8 |
| drink | 2020-04 | 182 | -54.0 |

## Exercise 5

```sql
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
```

| username | products |
|---|---|
| amy50 | 6 |
| amydavenport | 6 |
| beverlyterrell | 6 |
| brianromero | 6 |
| calebrussell | 6 |
| cassandra01 | 6 |
| cgrimes | 6 |
| charleslandry | 6 |
| clarence34 | 6 |
| danderson | 6 |
| davidalvarez | 6 |
| debra12 | 6 |
| dsanchez | 6 |
| ericolson | 6 |
| gabriellecameron | 6 |
| garzaanthony | 6 |
| heatherberger | 6 |
| hoganashlee | 6 |
| howelljohn | 6 |
| hritter | 6 |
| ilewis | 6 |
| jcervantes | 6 |
| jennifercollins | 6 |
| jford | 6 |
| jsmith | 6 |
| kimberly63 | 6 |
| kylestokes | 6 |
| lindsay78 | 6 |
| matthewmiranda | 6 |
| millerroy | 6 |

## Parquet export

```sql
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
```

| Count |
|---|
| 2538 |

## Query

```sql
SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');
```

| path_in_schema | type | compression | total_compressed_size | total_uncompressed_size |
|---|---|---|---|---|
| uuid | BYTE_ARRAY | SNAPPY | 91850 | 101542 |
| user_uuid | BYTE_ARRAY | SNAPPY | 2174 | 2469 |
| date | INT64 | SNAPPY | 17685 | 20326 |
| quantity | INT64 | SNAPPY | 1041 | 1044 |
| product | BYTE_ARRAY | SNAPPY | 1068 | 1065 |

## Hive partitioning

```sql
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product), OVERWRITE_OR_IGNORE);
```

| Count |
|---|
| 2538 |

## Query

```sql
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
```

| file |
|---|
| data/analytics/orders_by_product/product=bread/data_0.parquet |
| data/analytics/orders_by_product/product=brioche/data_0.parquet |
| data/analytics/orders_by_product/product=cookie/data_0.parquet |
| data/analytics/orders_by_product/product=croissant/data_0.parquet |
| data/analytics/orders_by_product/product=donut/data_0.parquet |
| data/analytics/orders_by_product/product=drink/data_0.parquet |

## Query

```sql
EXPLAIN ANALYZE
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
```

| explain_key | explain_value |
|---|---|
| analyzed_plan | ╭─ Summary ───────────╮ │ Total Time: 0.0413s │ │ Data Read:  32.7 kB │ ╰─────────────────────╯ ╭─ Ungrouped Aggregate ──────────────────────────────────────────╮ │ Aggregates: count_star()                                       │ │ 1 row                                                      0µs │ ╰────────────────────────────────┒┎──────────────────────────────╯ ╭─ Table Scan ───────────────────┚┖──────────────────────────────╮ │ Function: READ_PARQUET                                         │ │ File Filters: product = 'cookie'                               │ │ Scanning Files: 1/6                                            │ │ Total Files Read: 1                                            │ │ Filename(s):                                                   │ │ data/analytics/orders_by_product/product=cookie/data_0.parquet │ │ Row Groups Scanned: 1 / 1                                      │ │ 388 rows                                               130.0ms │ ╰────────────────────────────────────────────────────────────────╯  |
