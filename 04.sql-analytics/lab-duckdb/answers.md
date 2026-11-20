# Lab DuckDB: answers

Run on the Onyxia `vscode-pyspark` service against the bronze layer of the bucket `user-m-cuziat-ece` (DuckDB v1.5.5).
Files: [queries.sql](./queries.sql) (every query and exercise), [run-s3.sh](./run-s3.sh) (the full run: S3 secret,
queries, large dataset, Python API, cleanup) and `ece-2026-bigdata/src/ece_2026_bigdata/orders_report.py`.

## S3 configuration

- `duckdb_secrets()` lists the persistent secret `s3_onyxia_connection` (type `s3`, provider `config`, stored in
  `local_file`).
- The secret was regenerated with `CREATE PERSISTENT SECRET ... PROVIDER credential_chain` as described in the lab: the
  result is `Success: true`.
- The persistent secret is stored in a file of the home directory. The risk is that any process running as the user, or
  anyone who reads the file or a backup of the volume, can reuse the credentials. On Onyxia the risk is limited because
  the credentials are temporary (the session token expires after about 12 hours), are scoped to the personal bucket and
  the home directory belongs to a single-user container.
- For a Kubernetes Job, give the pod its own short-lived identity (a Secret mounted only in that pod with minimal
  permissions, or a workload identity / IAM role bound to its service account) instead of a file shared by the user.

## Bronze layer

- `sniff_csv` detects the delimiter `,`, the quote `"` and the header. `orders.csv` is typed `uuid`, `user_uuid`
  `VARCHAR`, `date` `TIMESTAMP WITH TIME ZONE`, `quantity` `BIGINT`, `product` `VARCHAR`.
- Tables loaded: **50 users and 2829 orders**, from 2020-01-01 00:02:17 to 2020-04-27 20:12:06.
- Why does `approx_unique` return 40 and 3167 instead of 50 users and 2829 orders? `approx_unique` is a HyperLogLog
  estimate: one pass, little memory, an error of a few percent. (`user_uuid` has 40 as an estimate of the 50 distinct
  values, `uuid` 3167 of 2829 real ones.) The exact value is given by `count(DISTINCT ...)`.
- Which issue may occur when the first rows of a large file are not representative? The types are inferred from a
  sample: a column that looks like an integer in the sample but holds text later makes the full read fail or get a wrong
  type. Fix: declare the types, increase `sample_size`, or read everything as `VARCHAR` and cast.

## Data quality checks

0 orphan orders, 0 inactive users and no duplicated order identifier, as expected.

## Exercises

1. Average orders per user: **56.58**. Average quantity per order: **3.05**.
2. First and last order per user and days between them: at most 5 days (`hoganashlee`, 2020-02-06 to 2020-02-11), then
   4 days for `wrightjames`, `karenhudson`, `snguyen` and `elizabeth57`. The generator places one order per hour, so a
   user only orders in a short window.
3. Hour with the highest quantity sold: **6h (385)**, then 15h (382) and 0h (379).
4. Month-over-month variation per product (`lag(quantity) OVER (PARTITION BY product ORDER BY month)`):

   | product | Feb | Mar | Apr |
   |---|---|---|---|
   | bread | +14.9 % | +13.5 % | -19.2 % |
   | brioche | -18.8 % | -4.9 % | -6.9 % |
   | cookie | -1.3 % | -8.4 % | -18.2 % |
   | croissant | +8.9 % | -1.8 % | +0.3 % |
   | donut | -28.3 % | +2.2 % | -1.2 % |
   | drink | -3.2 % | +10.1 % | -13.2 % |

5. Users who ordered the 6 products at least once: **45 of the 50 users** (`HAVING count(DISTINCT product) = 6`).

## Parquet export

`parquet_metadata` of `analytics/orders.parquet` (all columns SNAPPY): `uuid` 102191 bytes compressed for 113189
uncompressed, `user_uuid` 2125 for 2348, `date` 22667 for 22661, `quantity` 1242 for 1245, `product` 1271 for 1266.

- Why is the `uuid` column barely compressed? Every value is a distinct random 36-character string: there is nothing to
  deduplicate or run-length encode, and random data does not compress.
- Why is the compressed size of some columns larger than their uncompressed size? (`date`, `quantity`, `product`.)
  These columns are already compact and high-entropy, so the codec cannot reduce them and adds its own framing and
  page headers.

## Hive partitioning

`COPY ... (FORMAT parquet, PARTITION_BY (product))` writes one directory `product=<value>/` per product (6 files).
`EXPLAIN ANALYZE ... WHERE product = 'cookie'` reads 1 file out of 6.

- Why is partitioning by `uuid` a bad idea? One directory and one tiny file per order: millions of small objects.
  Object stores are slow at listing and at opening many small objects (one HTTP request per object, listing is
  paginated), and a tiny Parquet file is dominated by its footer.
- Which partition column for a dataset of orders growing every day? The order date (`year/month/day`): new data only
  creates new partitions, queries filter on time and each partition keeps a reasonable size.

## CSV vs. Parquet at scale

The large dataset (5,000 users) holds **252,416 orders**: the CSV file weighs **29.6 MB** (29,574,123 bytes) and the
Parquet file **11.5 MB** (11,532,863 bytes). Each query was run in a new process with the file cache disabled:

| Query | Data read (`in:`) | `#GET` | `Total Time` |
|---|---|---|---|
| CSV, `GROUP BY product` | 28.2 MiB | 1 | 1.11 s |
| Parquet, `GROUP BY product` | 203.5 KiB | 4 | 0.342 s |
| Parquet, `WHERE date >= '2100-01-01'` | 16.0 KiB | 1 | 0.311 s |

- How many row groups, and how many are skipped by the filter? The file has **3 row groups** (123,573, 124,276 and
  4,567 rows) whose dates end in 2048. The footer statistics (`stats_max`) show that none of them contains a date
  after 2100, so **all 3 are skipped** and only 16 KiB are read (the footer).
- What if the orders were shuffled? Every row group would hold dates from the whole range: the min/max statistics
  could not exclude any row group and the `date` pages of the whole file would be read.
- Which part of the difference is due to the network and which to the CSV parsing? The CSV query downloads the whole
  file, all 5 columns (28.2 MiB), then parses text. The Parquet query downloads only the footer and the 2 needed
  column chunks (203 KiB), already typed and compressed. The three queries are close in time here (0.3 to 1.1 s)
  because a part of each run is fixed: the S3 round trips (latency of 4 GETs against 1) and DuckDB start-up. The
  volume transferred, 140 times smaller, is the reproducible difference.

## Python API

`uv run orders-report` and `uv run orders-report -p cookie` print the monthly orders (744 / 696 / 744 / 645 orders,
and 128 / 121 / 122 / 95 for the cookies), identical to the reference of the lab. The product is passed as a query
parameter, not concatenated, which prevents SQL injection.

## Cleanup

`analytics/` and `large/` were removed from the bucket; the bronze layer is kept for the next modules.
