---
duration: 2h
category:
  - name: LAB
components:
  - name: DUCKDB
  - name: S3
  - name: PYTHON
platforms:
  - name: LINUX
resources:
  - title: DuckDB (official documentation)
    url: https://duckdb.org/docs/
  - title: DuckDB CLI (official documentation)
    url: https://duckdb.org/docs/stable/clients/cli/overview
  - title: DuckDB S3 API support (official documentation)
    url: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
  - title: DuckDB Python API (official documentation)
    url: https://duckdb.org/docs/stable/clients/python/overview
  - title: Apache Parquet (official documentation)
    url: https://parquet.apache.org/docs/
revisions:
  - date: 2026-09-17
    comment: Initial page
    author: pierre@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: SQL analytics with DuckDB

## Objectives

- Install and use the DuckDB CLI
- Query the CSV datasets of the bronze layer directly on S3
- Write analytical SQL queries: aggregations, joins, window functions, pivots
- Convert the datasets to Parquet and measure the benefits of a columnar format
- Run DuckDB queries from a Python script

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The `bronze/users.csv` and `bronze/orders.csv` objects uploaded at the end of the
  [S3 lab](../03.object-storage/lab-2-s3.md)

## Environment

Move into the project and set the environment variables of the S3 lab. As a reminder, the AWS CLI and `s5cmd` read the
credentials from the `default` profile of the AWS configuration files.

```bash
cd /home/onyxia/work
export S3_ENDPOINT_URL=$(
  aws configure get endpoint_url --profile 'default' \
  || echo "https://$AWS_S3_ENDPOINT"
)
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
echo "$S3_ENDPOINT_URL $LAB_BUCKET_NAME"
```

Check that the datasets are present in the bronze layer:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

If they are missing, generate and upload them again:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' cp orders.csv "s3://$LAB_BUCKET_NAME/bronze/orders.csv"
```

## DuckDB on Onyxia

The Onyxia images, including `vscode-pyspark`, come with the DuckDB CLI in `/usr/local/bin`. The `httpfs` and `aws`
extensions, used to access object storage, are already installed.

```bash
command -v duckdb
#> /usr/local/bin/duckdb
duckdb --version
#> v1.5.5 (Variegata) d8cdaa33fd
```

The version depends on the image of the service.

## First steps

Without argument, DuckDB starts with an in-memory database: everything is lost when the CLI exits.

```bash
duckdb
```

The prompt accepts SQL statements terminated by `;` and dot commands, specific to the CLI.

```sql
SELECT 'Hello DuckDB' AS greeting, 21 * 2 AS answer;
-- ┌──────────────┬────────┐
-- │   greeting   │ answer │
-- │   varchar    │ int32  │
-- ├──────────────┼────────┤
-- │ Hello DuckDB │     42 │
-- └──────────────┴────────┘
.help
.quit
```

Useful dot commands:

- `.help`: list the dot commands
- `.tables`: list the tables and their columns
- `.timer on`: print the execution time of each query
- `.mode line`: print one value per line, convenient for wide rows, `.mode duckbox` restores the default
- `.read file.sql`: execute the statements of a file
- `.quit` or `Ctrl+D`: exit

## S3 configuration

DuckDB connects to S3 with a secret, which holds the credentials, the endpoint and the URL style. When the service
starts, Onyxia creates a persistent secret named `s3_onyxia_connection` from the `AWS_*` environment variables. It is
stored in `~/.duckdb/stored_secrets` and loaded automatically by every DuckDB process of the user, the CLI as well as
the Python library.

```bash
duckdb -c "SELECT name, type, provider, persistent, storage FROM duckdb_secrets();"
#> ┌──────────────────────┬─────────┬──────────┬────────────┬────────────┐
#> │         name         │  type   │ provider │ persistent │  storage   │
#> │       varchar        │ varchar │ varchar  │  boolean   │  varchar   │
#> ├──────────────────────┼─────────┼──────────┼────────────┼────────────┤
#> │ s3_onyxia_connection │ s3      │ config   │ true       │ local_file │
#> └──────────────────────┴─────────┴──────────┴────────────┴────────────┘
```

The `secret_string` column displays the full configuration, with the secret key and the session token redacted. Check
the `endpoint` and the `url_style` values: with `path`, the bucket name is placed in the path of the URL,
`https://<endpoint>/<bucket>/<key>`, instead of the host name.

The credentials of Onyxia are temporary and the secret contains the values of the service startup. If a query fails
with an `ExpiredToken` or `403` error, restart the service to renew them.

Outside of Onyxia, the secret is created with the `CREATE SECRET` statement presented in the course.

Questions:

- The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on
  Onyxia?
- The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes
  Job?

## Initialization file

The statements executed at the start of each session are written in an initialization file. It is generated from the
terminal to inject your bucket name, and defines:

- a `bucket` variable, read in the queries with `getvariable('bucket')`, to avoid writing your bucket name in every
  query
- the `UTC` time zone, the CLI otherwise displays timestamps in the time zone of the machine

```bash
cat > init.sql <<SQL
SET VARIABLE bucket = 's3://$LAB_BUCKET_NAME';
SET TimeZone = 'UTC';
SQL
cat init.sql
```

A database file is created in the project. It persists the tables created during the lab. The `-init` argument executes
`init.sql` first:

```bash
duckdb analytics.duckdb -init init.sql
#> -- Loading resources from init.sql
```

```sql
SELECT getvariable('bucket') AS bucket;
-- ┌──────────────────┐
-- │      bucket      │
-- │     varchar      │
-- ├──────────────────┤
-- │ s3://user-gollum │
-- └──────────────────┘
```

The remaining SQL statements of the lab are executed in this CLI session, unless stated otherwise. The database file and
the generated CSV and Parquet files are not source code: add `*.duckdb` and `init.sql` to the `.gitignore` file of the
project.

## Query the bronze layer

The `read_csv` function reads a CSV file, locally or on S3, without loading it into the database. The objects are the
raw datasets of the bronze layer.

```sql
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bucket') || '/bronze/users.csv')
LIMIT 3;
-- ┌──────────────────────────────────────┬────────────────┬────────────────┬────────────┐
-- │                 uuid                 │    username    │      name      │ birthdate  │
-- │               varchar                │    varchar     │    varchar     │    date    │
-- ├──────────────────────────────────────┼────────────────┼────────────────┼────────────┤
-- │ bdd640fb-0667-4ad1-9c80-317fa3b1799d │ garzaanthony   │ Charles Garcia │ 1935-09-08 │
-- │ 17be3111-1a2a-43ed-962b-0f79c37459ee │ blairamanda    │ Ryan Munoz     │ 2009-12-04 │
-- │ 47294739-614f-43d7-99db-3ad0ddd1dfb2 │ elizabethmiles │ Jacob Wood     │ 1941-10-07 │
-- └──────────────────────────────────────┴────────────────┴────────────────┴────────────┘
```

CSV files carry no schema. DuckDB samples the file to detect the dialect (delimiter, quote, header) and the type of each
column. The `address` column contains line breaks: it is correctly parsed because its values are quoted.

```sql
SELECT Delimiter, Quote, HasHeader
FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv');
-- ┌───────────┬─────────┬───────────┐
-- │ Delimiter │  Quote  │ HasHeader │
-- │  varchar  │ varchar │  boolean  │
-- ├───────────┼─────────┼───────────┤
-- │ ,         │ "       │ true      │
-- └───────────┴─────────┴───────────┘
DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
-- ┌────────────────────────────────────┐
-- │              Describe              │
-- │                                    │
-- │ uuid      varchar                  │
-- │ user_uuid varchar                  │
-- │ date      timestamp with time zone │
-- │ quantity  bigint                   │
-- │ product   varchar                  │
-- └────────────────────────────────────┘
```

`SUMMARIZE` computes statistics for every column: minimum, maximum, approximate number of distinct values, average,
quartiles, and percentage of null values. The query selects a subset of them.

```sql
SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv'));
-- ┌─────────────┬──────────────────────────┬───────────────┬─────────────────┐
-- │ column_name │       column_type        │ approx_unique │ null_percentage │
-- │   varchar   │         varchar          │     int64     │  decimal(9,2)   │
-- ├─────────────┼──────────────────────────┼───────────────┼─────────────────┤
-- │ uuid        │ VARCHAR                  │          3167 │            0.00 │
-- │ user_uuid   │ VARCHAR                  │            40 │            0.00 │
-- │ date        │ TIMESTAMP WITH TIME ZONE │          3566 │            0.00 │
-- │ quantity    │ BIGINT                   │             5 │            0.00 │
-- │ product     │ VARCHAR                  │             6 │            0.00 │
-- └─────────────┴──────────────────────────┴───────────────┴─────────────────┘
```

Questions:

- The dataset contains 50 users, each one with at least one order, and 2829 orders with unique identifiers. Why does
  `approx_unique` return 40 and 3167?
- The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not
  representative?

## Load the tables

Every query on `read_csv` downloads and parses the file again. The datasets are loaded into tables of the
`analytics.duckdb` database, stored in the DuckDB columnar format.

```sql
CREATE OR REPLACE TABLE users AS
FROM read_csv(getvariable('bucket') || '/bronze/users.csv');
CREATE OR REPLACE TABLE orders AS
FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
.tables
```

`FROM read_csv(...)` is the DuckDB short form of `SELECT * FROM read_csv(...)`. Check the content of the tables:

```sql
SELECT
  (SELECT count(*) FROM users) AS users,
  (SELECT count(*) FROM orders) AS orders;
-- ┌───────┬────────┐
-- │ users │ orders │
-- │ int64 │ int64  │
-- ├───────┼────────┤
-- │    50 │   2829 │
-- └───────┴────────┘
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;
-- ┌───────────────────────────────┬───────────────────────────────┐
-- │          first_order          │          last_order           │
-- │   timestamp with time zone    │   timestamp with time zone    │
-- ├───────────────────────────────┼───────────────────────────────┤
-- │ 2020-01-01 00:02:17.891488+00 │ 2020-04-27 20:12:06.167622+00 │
-- └───────────────────────────────┴───────────────────────────────┘
```

A table is a copy of the data at the time it was loaded. A view, created with `CREATE VIEW ... AS FROM read_csv(...)`,
stores the query instead: it always reads the latest version of the file, at the cost of reading it on every query.

## Data quality checks

Before computing metrics, verify the assumptions made on the data. An `ANTI JOIN` returns the rows of the left table
which have no match in the right table.

```sql
-- Orders referencing an unknown user
SELECT count(*) AS orphan_orders
FROM orders o
ANTI JOIN users u ON o.user_uuid = u.uuid;
-- Users without any order
SELECT count(*) AS inactive_users
FROM users u
ANTI JOIN orders o ON o.user_uuid = u.uuid;
-- Duplicated order identifiers
SELECT uuid, count(*) AS occurrences
FROM orders
GROUP BY uuid
HAVING count(*) > 1;
```

The 3 queries return `0` or no row. In the next module, such checks are automated with dbt tests.

## Aggregations

Quantity sold per product, and its share of the total. `sum(sum(quantity)) OVER ()` is a window function applied after
the aggregation: it computes the total over all the groups.

```sql
SELECT
  product,
  count(*) AS orders,
  sum(quantity) AS quantity,
  round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders
GROUP BY product
ORDER BY quantity DESC;
-- ┌───────────┬────────┬──────────┬───────────┐
-- │  product  │ orders │ quantity │ share_pct │
-- │  varchar  │ int64  │  int128  │  double   │
-- ├───────────┼────────┼──────────┼───────────┤
-- │ croissant │    491 │     1523 │      17.7 │
-- │ drink     │    484 │     1493 │      17.3 │
-- │ brioche   │    468 │     1424 │      16.5 │
-- │ bread     │    458 │     1420 │      16.5 │
-- │ donut     │    462 │     1390 │      16.1 │
-- │ cookie    │    466 │     1365 │      15.8 │
-- └───────────┴────────┴──────────┴───────────┘
```

Orders per month. `date_trunc` truncates a timestamp to the requested precision.

```sql
SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders
GROUP BY month
ORDER BY month;
-- ┌──────────────────────────┬────────┬──────────┐
-- │          month           │ orders │ quantity │
-- │ timestamp with time zone │ int64  │  int128  │
-- ├──────────────────────────┼────────┼──────────┤
-- │ 2020-01-01 00:00:00+00   │    744 │     2297 │
-- │ 2020-02-01 00:00:00+00   │    696 │     2154 │
-- │ 2020-03-01 00:00:00+00   │    744 │     2192 │
-- │ 2020-04-01 00:00:00+00   │    645 │     1972 │
-- └──────────────────────────┴────────┴──────────┘
```

The generator creates exactly one order per hour: 744 orders in the 31 days of January, 696 in the 29 days of
February 2020.

## Joins

The top 5 customers, joining the `orders` fact table with the `users` dimension. `GROUP BY ALL` groups by all the
columns which are not aggregated, `username` and `name`.

```sql
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY quantity DESC
LIMIT 5;
-- ┌─────────────┬────────────────────┬────────┬──────────┐
-- │  username   │        name        │ orders │ quantity │
-- │   varchar   │      varchar       │ int64  │  int128  │
-- ├─────────────┼────────────────────┼────────┼──────────┤
-- │ elizabeth57 │ Connie Holt        │     97 │      317 │
-- │ karenhudson │ Angelica Keith     │     97 │      303 │
-- │ jsmith      │ Shane Alexander    │    100 │      301 │
-- │ hoganashlee │ Patricia Jefferson │    100 │      289 │
-- │ stephen00   │ Connie Gilbert     │     92 │      285 │
-- └─────────────┴────────────────────┴────────┴──────────┘
```

Orders per age group of the customers, computed at the date of the first order. The `//` operator is the integer
division.

```sql
SELECT
  (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
  count(DISTINCT u.uuid) AS users,
  count(*) AS orders,
  round(avg(o.quantity), 2) AS avg_quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group
ORDER BY age_group;
-- ┌───────────┬───────┬────────┬──────────────┐
-- │ age_group │ users │ orders │ avg_quantity │
-- │   int64   │ int64 │ int64  │    double    │
-- ├───────────┼───────┼────────┼──────────────┤
-- │         0 │    14 │    686 │         3.08 │
-- │        20 │     7 │    483 │         3.02 │
-- │        40 │    12 │    721 │         3.02 │
-- │        60 │    11 │    694 │         3.02 │
-- │        80 │     5 │    198 │         3.14 │
-- │       100 │     1 │     47 │         3.19 │
-- └───────────┴───────┴────────┴──────────────┘
```

The random birth dates are not realistic: the silver layer is the place where such values are validated.

## Window functions

A window function computes a value for each row over a set of rows, the window, defined by the `OVER` clause.

Daily quantity, cumulative quantity, and moving average over the last 7 days. The `daily` common table expression (CTE)
aggregates the orders per day, the window functions are then applied to the daily rows.

```sql
WITH daily AS (
  SELECT date::DATE AS day, sum(quantity) AS quantity
  FROM orders
  GROUP BY day
)
SELECT
  day,
  quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily
ORDER BY day
LIMIT 10;
-- ┌────────────┬──────────┬────────────┬────────┐
-- │    day     │ quantity │ cumulative │ avg_7d │
-- │    date    │  int128  │   int128   │ double │
-- ├────────────┼──────────┼────────────┼────────┤
-- │ 2020-01-01 │       86 │         86 │   86.0 │
-- │ 2020-01-02 │       62 │        148 │   74.0 │
-- │ 2020-01-03 │       80 │        228 │   76.0 │
-- │ 2020-01-04 │       66 │        294 │   73.5 │
-- │ 2020-01-05 │       72 │        366 │   73.2 │
-- │ 2020-01-06 │       75 │        441 │   73.5 │
-- │ 2020-01-07 │       73 │        514 │   73.4 │
-- │ 2020-01-08 │       81 │        595 │   72.7 │
-- │ 2020-01-09 │       89 │        684 │   76.6 │
-- │ 2020-01-10 │       75 │        759 │   75.9 │
-- └────────────┴──────────┴────────────┴────────┘
```

The best-selling product of each month. `rank()` numbers the products of each month, `PARTITION BY`, by descending
quantity. `QUALIFY` filters on the result of a window function, like `HAVING` filters on the result of an aggregation.

```sql
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders
GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;
-- ┌─────────┬───────────┬──────────┐
-- │  month  │  product  │ quantity │
-- │ varchar │  varchar  │  int128  │
-- ├─────────┼───────────┼──────────┤
-- │ 2020-01 │ donut     │      438 │
-- │ 2020-02 │ croissant │      392 │
-- │ 2020-03 │ bread     │      411 │
-- │ 2020-04 │ croissant │      386 │
-- └─────────┴───────────┴──────────┘
```

## Pivot

`PIVOT` turns the distinct values of a column into columns. The result is the quantity of each product per month, a
format ready for a report or a spreadsheet.

```sql
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product
USING sum(quantity)
ORDER BY month;
-- ┌─────────┬────────┬─────────┬────────┬───────────┬────────┬────────┐
-- │  month  │ bread  │ brioche │ cookie │ croissant │ donut  │ drink  │
-- │ varchar │ int128 │ int128  │ int128 │  int128   │ int128 │ int128 │
-- ├─────────┼────────┼─────────┼────────┼───────────┼────────┼────────┤
-- │ 2020-01 │    315 │     431 │    376 │       360 │    438 │    377 │
-- │ 2020-02 │    362 │     350 │    371 │       392 │    314 │    365 │
-- │ 2020-03 │    411 │     333 │    340 │       385 │    321 │    402 │
-- │ 2020-04 │    332 │     310 │    278 │       386 │    317 │    349 │
-- └─────────┴────────┴─────────┴────────┴───────────┴────────┴────────┘
```

## Exercises

Write the queries answering the following questions:

1. What is the average number of orders per user, and the average quantity per order?
2. For each user, what is the date of their first and last order, and the number of days between them?
3. Which hour of the day has the highest quantity sold? Use the `hour` function.
4. For each product, what is the month-over-month variation of the quantity sold, in percent? Use the `lag` window
   function.
5. Which users ordered every product at least once?

## Parquet export

The `COPY` statement exports a table or a query to a file. The `orders` table is written to S3 in the Parquet format.

```sql
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
```

The metadata of a Parquet file is stored in its footer. `parquet_metadata` reads it and returns one row per column
chunk, with its type, compression codec, size and statistics.

```sql
SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');
-- ┌────────────────┬────────────┬─────────────┬───────────────────────┬─────────────────────────┐
-- │ path_in_schema │    type    │ compression │ total_compressed_size │ total_uncompressed_size │
-- │    varchar     │  varchar   │   varchar   │         int64         │          int64          │
-- ├────────────────┼────────────┼─────────────┼───────────────────────┼─────────────────────────┤
-- │ uuid           │ BYTE_ARRAY │ SNAPPY      │                102191 │                  113189 │
-- │ user_uuid      │ BYTE_ARRAY │ SNAPPY      │                  2125 │                    2348 │
-- │ date           │ INT64      │ SNAPPY      │                 22667 │                   22661 │
-- │ quantity       │ INT64      │ SNAPPY      │                  1242 │                    1245 │
-- │ product        │ BYTE_ARRAY │ SNAPPY      │                  1271 │                    1266 │
-- └────────────────┴────────────┴─────────────┴───────────────────────┴─────────────────────────┘
```

The file weighs 130 KB against 332 KB for the CSV file. Timestamps and integers are stored in binary, and the
`user_uuid` and `product` columns, with few distinct values, are dictionary encoded: 2829 identifiers of 36 characters
are stored in 2 KB.

Questions:

- Why is the `uuid` column barely compressed?
- Why is the compressed size of some columns larger than their uncompressed size?

## Hive partitioning

With the `PARTITION_BY` option, `COPY` writes one directory per value of the partition column. The directory names
follow the Hive convention, `<column>=<value>`.

```sql
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product));
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
-- ┌───────────────────────────────────────────────────────────────────────────────┐
-- │                                     file                                      │
-- │                                    varchar                                    │
-- ├───────────────────────────────────────────────────────────────────────────────┤
-- │ s3://user-gollum/analytics/orders_by_product/product=bread/data_0.parquet     │
-- │ s3://user-gollum/analytics/orders_by_product/product=brioche/data_0.parquet   │
-- │ s3://user-gollum/analytics/orders_by_product/product=cookie/data_0.parquet    │
-- │ s3://user-gollum/analytics/orders_by_product/product=croissant/data_0.parquet │
-- │ s3://user-gollum/analytics/orders_by_product/product=donut/data_0.parquet     │
-- │ s3://user-gollum/analytics/orders_by_product/product=drink/data_0.parquet     │
-- └───────────────────────────────────────────────────────────────────────────────┘
```

The `product` column is not stored in the files anymore, its value is read from the path. When the query filters on the
partition column, the files of the other partitions are skipped. `EXPLAIN ANALYZE` executes the query and prints the
plan with the statistics of each operator.

```sql
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
```

Look at the `TABLE_SCAN` operator at the bottom of the plan:

```text
┌─────────────┴─────────────┐
│         TABLE_SCAN        │
│    ────────────────────   │
│         Function:         │
│        READ_PARQUET       │
│                           │
│       File Filters:       │
│    (product = 'cookie')   │
│                           │
│    Scanning Files: 1/6    │
│    Total Files Read: 1    │
...
│          466 rows         │
└───────────────────────────┘
```

Only 1 of the 6 files is read.

Questions:

- Why is partitioning by `uuid` a bad idea? Refer to the limitations of object storage presented in the previous module.
- Which partition column would you choose for a dataset of orders growing every day?

## CSV vs. Parquet at scale

The bronze datasets are too small to observe a difference of performance. Exit the CLI with `.quit`, generate a dataset
of about 1 million orders for 20,000 users, and upload it to the `large/` prefix. The generation takes about 30 seconds.
The orders are placed one hour apart: the dates span more than a century.

```bash
uv run dataset-orders -u 20000 -o csv > orders_large.csv
ls -lh orders_large.csv
#> -rw-r--r-- 1 onyxia users 112M Sep 17 10:00 orders_large.csv
s5cmd --profile 'default' cp orders_large.csv "s3://$LAB_BUCKET_NAME/large/orders.csv"
```

Open the CLI again, and convert the file to Parquet. DuckDB streams the CSV file from S3 and uploads the Parquet file
with a multipart upload.

```bash
duckdb analytics.duckdb -init init.sql
```

```sql
.timer on
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv'))
TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');
-- ┌──────────────┐
-- │ count_star() │
-- │    int64     │
-- ├──────────────┤
-- │       999490 │
-- └──────────────┘
.quit
```

Compare the sizes of the objects:

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/large/"
#> 2026-09-17 10:01:12  117105243 orders.csv
#> 2026-09-17 10:02:05   45668392 orders.parquet
```

DuckDB caches the files read from S3 in memory during a session. To measure the data transferred by each query, run
each query in a new CLI process with the cache disabled. The `HTTPFS HTTP Stats` box at the top of the `EXPLAIN ANALYZE`
output reports the number of requests and the volume of data received.

```bash
for query in \
  "FROM read_csv(getvariable('bucket') || '/large/orders.csv') SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT count(*) WHERE date >= '2100-01-01'"
do
  echo "$query"
  duckdb -init init.sql -c "SET enable_external_file_cache = false; EXPLAIN ANALYZE $query" 2>/dev/null \
  | grep -E 'in:|#GET|Total Time'
done
```

```text
FROM read_csv(getvariable('bucket') || '/large/orders.csv') SELECT product, sum(quantity) GROUP BY product
││           in: 111.6 MiB           ││
││              #GET: 4              ││
││              Total Time: 0.282s              ││
FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT product, sum(quantity) GROUP BY product
││           in: 806.3 KiB           ││
││              #GET: 10             ││
││              Total Time: 0.144s              ││
FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT count(*) WHERE date >= '2100-01-01'
││            in: 2.9 MiB            ││
││              #GET: 5              ││
││              Total Time: 0.0806s             ││
```

The times above were measured with an S3 server on the same machine: on the platform, they depend on the network and
on its load, and the gap between CSV and Parquet widens. The volumes of data are reproducible:

- The CSV query downloads the whole file, 111.6 MiB, to read 2 columns.
- The Parquet query downloads the footer, then only the column chunks of `product` and `quantity`, 806 KiB. The
  `product` column is dictionary encoded and the `quantity` column contains small integers: both are highly compressed.
- The filtered query reads the `date` column of the row groups which may contain dates after 2100 only.

The row groups skipped by the filter are identified with the statistics of the footer:

```bash
duckdb -init init.sql -c "
  SELECT row_group_id, row_group_num_rows, stats_min, stats_max
  FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet')
  WHERE path_in_schema = 'date'
"
```

```text
┌──────────────┬────────────────────┬───────────────────────────────┬───────────────────────────────┐
│ row_group_id │ row_group_num_rows │           stats_min           │           stats_max           │
│    int64     │       int64        │            varchar            │            varchar            │
├──────────────┼────────────────────┼───────────────────────────────┼───────────────────────────────┤
│            0 │             123577 │ 2020-01-01 00:50:06.508418+00 │ 2034-02-05 00:42:57.17276+00  │
│            1 │             124273 │ 2034-02-05 01:39:15.688608+00 │ 2048-04-10 01:11:47.691839+00 │
...
```

Questions:

- How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?
- The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?
- Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which
  part to the parsing of the CSV file?

## Python API

DuckDB is also a Python library. The query runs inside the Python process, there is no server to connect to. Add the
dependency to the project:

```bash
uv add duckdb
```

The `orders_report.py` script prints the monthly orders, optionally filtered on a product. The Python library loads the
`s3_onyxia_connection` persistent secret like the CLI, and the bucket name is read from the environment variables. The
product is passed as a query parameter, `$product`, instead of being concatenated into the SQL string, which prevents
SQL injection.

```bash
cat <<'PY' >src/work/orders_report.py
import argparse
import os

import duckdb


def orders_report(product=None):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    bronze = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"
    return con.execute(
        f"""
        SELECT strftime(date, '%Y-%m') AS month, count(*) AS orders, sum(quantity) AS quantity
        FROM read_csv('{bronze}/orders.csv')
        WHERE $product IS NULL OR product = $product
        GROUP BY month
        ORDER BY month
        """,
        {"product": product},
    ).fetchall()


def main():
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Filter the orders on a product.")
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product):
        print(f"{month}\t{orders}\t{quantity}")


if __name__ == "__main__":
    main()
PY
```

Declare the command in the `[project.scripts]` section of `pyproject.toml`:

```toml
[project.scripts]
dataset-users = "work.dataset_users:main"
dataset-orders = "work.dataset_orders:main"
orders-report = "work.orders_report:main"
```

Run the report:

```bash
uv run orders-report
#> 2020-01	744	2297
#> 2020-02	696	2154
#> 2020-03	744	2192
#> 2020-04	645	1972
uv run orders-report -p cookie
#> 2020-01	128	376
#> 2020-02	121	371
#> 2020-03	122	340
#> 2020-04	95	278
```

`fetchall` returns a list of tuples. The results can also be converted without copy to other libraries, for example
with `.df()` for Pandas, `.pl()` for Polars, or `.arrow()` for PyArrow, when they are installed.

Commit the changes:

```bash
git add \
  .gitignore \
  pyproject.toml \
  uv.lock \
  src
git commit -m "feat: orders report with duckdb"
git push
```

## Cleanup

Remove the objects created during this lab. The objects of the bronze layer are kept for the next module.

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/analytics/" --recursive
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/large/" --recursive
rm -f orders_large.csv
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
#> 2026-09-14 11:02:10     331568 bronze/orders.csv
#> 2026-09-14 11:02:09       7351 bronze/users.csv
```

The `analytics.duckdb` database file can be deleted as well, the tables are recreated from the bronze layer at any time.
