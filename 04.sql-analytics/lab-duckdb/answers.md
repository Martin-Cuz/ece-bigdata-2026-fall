# Lab DuckDB: answers

## How this lab was run

The Onyxia cluster was unavailable when this lab was done (S3 credentials expired and the Kubernetes API timing out).
The lab was therefore run **locally** with the DuckDB Python library: the bucket `s3://<bucket>` is replaced by the local
folder `ece-2026-bigdata/data/` (`bronze/`, `analytics/`, `large/`). The SQL is the one of the lab, the only differences
are `strict_mode = false` on every `read_csv` and the `bucket` variable set to `data` in `init.sql`. The S3 secret,
`httpfs` and the `#GET` / `in:` HTTP statistics are not applicable locally.

The Faker version installed locally differs from the one used to write the lab, so the generated datasets are not
byte-identical: 2538 orders instead of 2829. The numbers below are the ones of this run. The full output of every query
is in [results.md](./results.md), the CSV vs Parquet measurements in [large-results.txt](./large-results.txt).

Files: `queries.sql` (all the queries and exercises), `run.py` (executes them), `large.py` (CSV vs Parquet),
`ece-2026-bigdata/src/ece_2026_bigdata/orders_report.py` (Python API).

## Query the bronze layer

- Why does `approx_unique` return 49 for `user_uuid` and 2500 for the order `uuid`, while the dataset has 2538 distinct order identifiers (the 49 users with orders are exact)? And why is `user_uuid` at 49 and not 50?
  `approx_unique` uses the HyperLogLog algorithm: a probabilistic estimate that needs little memory and a single pass,
  with an error of a few percent (2500 vs 2538 here). It is not an exact `count(DISTINCT ...)`. `user_uuid` shows 49 because the 50th user has no order.
- Which issue may occur when the first rows of a large file are not representative?
  The types are inferred from a sample. A column that looks like an integer in the sample but contains text later makes
  the full read fail, or is cast to a wrong type. Fix: declare the types explicitly (`types = {...}`), increase
  `sample_size`, or read everything as `VARCHAR` then cast.

## Data quality checks

- Orphan orders: 0. Duplicated order uuids: none.
- Inactive users: 1 (the lab text expects 0, the difference comes from the different random stream of this Faker
  version).

## Exercises

1. Average orders per user: 51.8 (2538 orders / 49 active users). Average quantity per order: 3.02.
2. First order, last order and days between them per user: see `Exercise 2` in `results.md` (the users with the
   largest span are `clarence34`, `jcervantes` and `ilewis`, 4 days). The generator produces one order per hour, so
   each user only orders in a short window.
3. Hour with the highest quantity: 19h (350 items), then 0h (349) and 20h (332).
4. Month-over-month variation per product: `lag(quantity) OVER (PARTITION BY product ORDER BY month)`. The last month is
   incomplete (the data stops on April 15), hence the drop of about -50 % in April for every product.
5. Users who ordered all 6 products at least once: see `Exercise 5` in `results.md`.

## Parquet export

- Why is the `uuid` column barely compressed? Every value is a distinct random 36-character string: there is no
  repetition to exploit for dictionary or run-length encoding, and random data does not compress.
- Why can the compressed size of some columns be larger than the uncompressed size? Small columns of few values
  (`quantity`, `product`): the codec adds its own framing and page headers, and compression cannot reduce already
  tiny, high-entropy data.
- Orders: the Parquet file is much smaller than the CSV (see below).

## Hive partitioning

- Why is partitioning by `uuid` a bad idea? Each order would get its own directory and file: thousands of tiny objects.
  Object stores are slow at listing and opening many small objects (one request per object, listing is paginated), and
  a tiny Parquet file is dominated by its footer and metadata.
- Which partition column for an orders dataset that grows daily? The order date (`year/month/day`, or `date`): new data
  only creates new partitions, queries filter on time, and each partition keeps a reasonable size. A low-cardinality
  business column such as `product` is a good second level.
- `EXPLAIN ANALYZE` with `WHERE product = 'cookie'` shows `Scanning Files: 1/6`, `Total Files Read: 1`: only the
  `product=cookie` file is read (388 rows).

## CSV vs. Parquet at scale

Run on a local disk, 249,265 orders for 5,000 users (see `large-results.txt`):

| Format | Size | `GROUP BY product` (best of 3) |
|---|---|---|
| CSV | 27.5 MB | 0.093 s |
| Parquet | 11.1 MB | 0.003 s |

- Number of row groups: 3 (123,820, 124,760 and 685 rows). With the filter `date >= '2100-01-01'`, the footer
  statistics (`stats_max` of the last row group is in 2048) let DuckDB skip all 3 row groups: the query takes 1 ms.
- If the orders were shuffled, every row group would contain dates from the whole range: min/max statistics would no
  longer exclude any row group, and the filter would have to read all the `date` pages.
- Time difference: locally there is no network, so the gap (about 30x) is almost entirely the CSV parsing and the
  reading of all the columns. On S3, the transfer of the whole CSV file (all columns) would add the network cost on
  top, while Parquet only downloads the footer and the two needed column chunks.

## Python API

`orders_report.py` prints the monthly orders, optionally filtered by product, with the product passed as a query
parameter (`$product`) to prevent SQL injection. The data location comes from `BRONZE_URI` (local folder) or from
`LAB_BUCKET_NAME` (S3):

```text
$ BRONZE_URI=data/bronze uv run orders-report -p cookie
2020-01	114	350
2020-02	104	321
2020-03	104	273
2020-04	66	187
```

## Parts not done

- S3 secret (`s3_onyxia_connection`) and the two questions about it, reading/writing on `s3://`, the `HTTPFS` request
  statistics, and the S3 cleanup: they require a working Onyxia service.
