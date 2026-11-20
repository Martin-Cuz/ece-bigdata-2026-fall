# Lab Spark DataFrames: answers

Run on the Onyxia `vscode-pyspark` service (Spark 4.1.1, Scala 2.13, `local[*]`, limits raised to 4 CPU and 4 Gi as the
lab requires). The whole lab is in [silver_lab.py](./silver_lab.py), launched by [run-silver.sh](./run-silver.sh) with
`spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`.

## Setup notes

- The first run killed the pod (out of memory with the default 2 Gi limit): the resources were raised with
  `kubectl set resources` as the lab explains, which restarts the pod. Only `/home/onyxia/work` survives a restart,
  so the scripts and logs live there.
- With `spark-submit`, `spark.jars.packages` set in the Python code is ignored because the JVM already runs: the
  Iceberg runtime must be given with `--packages`, otherwise `Cannot find catalog plugin class for catalog 'lab'`.

## SparkSession

- The credentials are temporary. If the service restarts in the middle of a job, the job dies with the pod, and the
  new pod receives new variables: a session started with the old values then fails with `ExpiredToken` /
  `InvalidAccessKeyId` as soon as it touches S3, because the credentials are copied into the Hadoop configuration at
  session creation.
- `SimpleAWSCredentialsProvider` only knows the access key and the secret key. The credentials of Onyxia are STS
  credentials: S3 also requires the session token, so the signature is rejected. `TemporaryAWSCredentialsProvider`
  sends the token with every request.

## Bronze and silver

- `users.csv` is read with `multiLine=True`: 50 users and 2829 orders, and the address keeps its line break (street,
  then city, state and zip code). Military addresses (`DPO AP 09617`) have no city: `city` is `NULL` after the
  `when(city == "", None)` fix.
- Deduplication: **2829 orders, 2832 with the 3 duplicates injected, 2829 after the window function**, and no order id
  left with more than one row. The Spark SQL version returns the same count and the same columns.
- Explanation of the output: the duplicate check lists the 3 order ids that appear twice, the same check after the
  `row_number()` filter is empty, and the SQL query is only another way to write the same plan.
- DataFrame or SQL? SQL is the best for a one-off exploration (short and readable by everybody); the DataFrame API
  suits a model reused by several people because it can be split into functions, tested, composed and refactored in
  Python without string building. Both compile to the same Catalyst plan.
- `regexp_extract()` returns an empty string, not `NULL`, when there is no match: the same trap exists for
  `F.trim`/`F.split` results, for `state` and `zip_code` of military addresses without city (an empty string would be
  counted as a group by `groupBy`), and in every `coalesce` or `isNull` test that expects `NULL`.

## Parquet and Iceberg

- `silver/spark/users.parquet` and `orders.parquet` are directories with 1 part-file each: the DataFrames have a single
  partition, because the CSV files are small (one Spark task per partition writes one file). `coalesce(1)` on a larger
  dataset forces one task to write everything: no parallelism in the write and a memory bottleneck.
- The Parquet files read back give 50 users and 2829 orders, the same counts as the bronze layer.
- Iceberg: `lab.silver.orders` has one `overwrite` snapshot with a data file of 2829 records (84,055 bytes), and
  `lab.silver.users` 50 records (7,434 bytes). The `warehouse/silver/orders/` directory holds `data/` (Parquet),
  `metadata/` with `v1.metadata.json`, the manifest `*-m0.avro`, the snapshot list `snap-*.avro` and
  `version-hint.text`; the data files hold the rows, everything else describes the table.
- A second job overwrites a Parquet directory while a query reads it: the reader can see missing or mixed files, or
  fail on a file that disappeared. With Iceberg, the reader works on one snapshot, the new files are invisible until the
  commit atomically replaces the pointer to the new metadata file.
- The pointer to the current version is a file in the bucket (`version-hint.text`, `vN.metadata.json`): two jobs
  committing at the same time can both write `v2` and one commit is lost or overwritten, because object stores have no
  atomic "compare and swap" on a key. A catalog service (REST catalog such as Polaris, Hive, JDBC) serializes the
  commits.

## Comparison with dbt

| | dbt (DuckDB) | Spark |
|---|---|---|
| Orders / total quantity | 2829 / 8615 | 2829 / 8615 |
| Users | 50 | 50 |
| Layout | one Parquet object per model | one directory of part-files |
| `order_id` / `user_id` type | `UUID` | `string` (Spark has no UUID type) |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | timestamp |

The two engines produce the same data; consumers of the Spark output must cast the identifiers to `UUID` themselves.

- Output layout: a single object is the easiest to read, but a directory of part-files scales (each task writes its
  own file in parallel, the consumer reads them as one dataset).
- What replaces the dbt tests in the notebook: manual checks with `count()` and `assert`. The same guarantees need
  code: `groupBy().count()` for `unique`, `filter(isNull)` for `not_null`, `isin` for `accepted_values` and an anti-join
  for `relationships`, or a library (Great Expectations, Deequ, Soda).
- What plays the role of `ref()` and `source()` in the notebook: the order of the cells and the variables
  (`users_bronze`, `stg_users`...); there is no graph, no automatic ordering and no lineage.
- Time to build: dbt took 3.9 s for the models and tests, the Spark cells a few seconds each, but the numbers are not
  comparable: Spark includes the JVM startup, the Ivy download of the Iceberg package and the S3 connection, and dbt
  runs inside one process on a few thousand rows.
- Which engine for which case: a daily batch of 10 GB fits on one machine, so dbt with DuckDB is simpler and cheaper;
  a daily batch of 10 TB needs distributed processing, so Spark; a team of analysts who only know SQL should use dbt
  with SQL (Spark SQL on a cluster is an option if the data is huge).

## Exercises

1. `birthdate_is_valid`: **5 invalid birth dates out of 50 users**, with the same count in the DataFrame version (join
   with the first order date, `coalesce(..., True)`) and in the SQL version; this is the same 5 users found by dbt.
2. Zip code extraction on `users_bronze`: `regexp_extract` 0.19 s, Python UDF 2.10 s, `pandas_udf` 1.26 s. The native
   function stays in the JVM and is optimized by Catalyst; a Python UDF serializes every row between the JVM and Python;
   the `pandas_udf` moves data in Arrow batches, which is faster than the row UDF but still slower than the built-in.
3. Cache: `groupBy` on the orders CSV takes 0.28, 0.18 and 0.18 s without cache, and 0.17, 0.15 and 0.14 s with cache.
   The files are tiny, so the gain is small; with a cache the CSV is no longer re-read from S3 for the later stages.
4. Iceberg append and time travel: after the append, the table has 2 snapshots (`overwrite`, `append`); the table has
   **2830 rows** now and **2829** `VERSION AS OF` the first snapshot. A Parquet directory has no history: rewriting or
   appending files changes what every reader sees and the previous state cannot be queried.

## Not done

The final step of the lab that commits the Iceberg `metadata/` files into Git needs the object-store files on the
machine where the repository is pushed; the pod has no GitHub credentials, so the warehouse metadata was only listed
and inspected on S3.
