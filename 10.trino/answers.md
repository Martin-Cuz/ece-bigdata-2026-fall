# Trino labs: answers

Run on the Onyxia `vscode-pyspark` service (limits 2 CPU and 2000 Mi as the lab requires), Trino 480 deployed with Helm in
the namespace `user-m-cuziat-ece`, reading the silver tables through the Polaris catalog of the previous module.

## Lab 1: deployment

Files: [lab-1-deployment/trino/deployment](./lab-1-deployment/trino/deployment) (the chart of the lab) and
[run-deploy.sh](./lab-1-deployment/run-deploy.sh) (secrets, ConfigMap, `helm upgrade --install`, checks).

- The coordinator-only Trino (`workers: 0`) starts and `curl http://trino:8080/v1/info` returns `HTTP/1.1 200 OK`
  with `{"nodeVersion":{"version":"480"},"starting":false,"coordinator":true}`.
- The secret `trino-polaris-oauth` holds the credential of the Polaris `dev` principal, `polaris-s3` the S3 keys and
  `trino-polaris-config` the catalog name; none of these values is stored in the repository.
- The CPU request of the coordinator was lowered from 250m to 10m in `values.yaml`: as for Polaris, the cluster has no
  free CPU to reserve, and a request is only what the scheduler reserves (the limits are unchanged).
- Before this lab, `polaris.silver.users` was dropped by the cleanup of the Polaris lab, so the table was registered
  again in Polaris with `register_table` (no data copied): Trino needs both `orders` and `users` in Polaris.

## Lab 2: silver to gold

Files: [lab-2/trino_lab.py](./lab-2/trino_lab.py) (labs 2 and 3 in one script, with the exercises),
[lab-2/spark_compare.py](./lab-2/spark_compare.py) and [lab-2/run-trino.sh](./lab-2/run-trino.sh).

### Connection, catalogs, schemas

- What does the `catalog=` parameter change? It sets the default catalog: names without a catalog prefix are resolved
  in `lakehouse` (so `silver.orders` would work), while fully qualified names such as `system.runtime.queries` are not
  affected.
- `SHOW CATALOGS` lists `lakehouse`, `system`, `tpcds`, `tpch`. `SHOW SCHEMAS FROM lakehouse` lists
  `information_schema`, `silver`, `system` and `silver` contains `orders` and `users`.
- Types: `ordered_at` is `timestamp(6) with time zone` (the time zone is kept, microsecond precision); `order_id` and
  `user_id` are `varchar`: Trino has a `UUID` type, but the Iceberg table stores strings (Spark wrote them as
  `string`), so Trino reports what the table says.

### Silver layer

- 2829 orders and 50 users (the same numbers as the Spark and dbt labs).
- Address parsing: `AE` 3 users with 3 missing cities, `FM` 3 / 0, `AP` 2 / 2, then states with 2 users and a city:
  the military addresses (`AE`, `AP`, `AA`) are the ones without a city, as in the Spark lab. `count_if` is the Trino
  counterpart of `sum(CASE WHEN ... THEN 1 ELSE 0 END)`.
- Orphan orders: **0**, so an inner join loses no order. This query replaces the `relationships` test of dbt; to run it
  on every refresh, put it in the pipeline as a check that fails when the result is greater than 0 (a singular test in
  dbt with the Trino adapter, or a step of the orchestrator).
- Trino answered with no job to submit and no session to create: an HTTP query service suits interactive and
  short analytical SQL (dashboards, exploration); Spark remains the tool for large transformations.

### Iceberg metadata

- `"orders$snapshots"` returns **1 snapshot** (`overwrite`, id 1333229184899724736), written by **Spark** (the table
  was created in the Spark lab and registered in Polaris); `"orders$files"` lists **1 Parquet file**, 2829 records,
  84,055 bytes, and `"orders$history"` one row. They are the same data as `lab.silver.orders.snapshots` and `.files`
  of Spark, read from the same metadata located by Polaris.
- `aws s3 ls` needs the credentials of the bucket and lists objects, whereas `$files` only needs a SQL connection to
  Trino: Trino reads the metadata with its own credentials and the user never touches the storage.

### Plan and execution

The distributed plan of the join has 4 fragments:

- Fragment 0 (`HASH`): the final `Aggregate` and a `RemoteSource`, the output stage;
- Fragment 1 (`SOURCE`): the partial `Aggregate` and an **`InnerJoin ... distribution = REPLICATED`**: the small table
  (50 users) is broadcast to the node scanning the orders;
- Fragment 2: the `ScanFilter` of `silver.orders` with a dynamic filter on `user_id`;
- Fragment 3: the `TableScan` of `silver.users`.

The exchanges between fragments are the `RemoteSource` operators (data crossing the network between stages) and the
`LocalExchange` inside a stage. `EXPLAIN ANALYZE` reports for the orders scan 2829 input rows (124 kB, 82 kB of physical
Parquet read) and for the users scan 50 rows (7 kB). In `system.runtime.queries`, the `EXPLAIN` took 1.6 s and the
`EXPLAIN ANALYZE` 4.0 s: for such small data, the time is dominated by planning, the access to the catalog and the
metadata.

### Gold layer

| Table | Rows written |
|---|---|
| `gold.orders_by_state_month` | 50 |
| `gold.top_products` | 6 |
| `gold.user_activity` | 50 |

`top_products`: croissant (491 orders, 1523 units, rank 1), drink (1493, 2), brioche (1424, 3), bread (1420, 4), donut
(1390, 5), cookie (1365, 6). Validation: the units are **8615** in the silver table, in `orders_by_state_month` and in
`top_products`, and `user_activity` has 50 users and 2829 orders, so every assertion passes.

- Why `LEFT JOIN` for `user_activity` and an inner join for `orders_by_state_month`? `user_activity` must keep the users
  who never ordered (an inner join would drop them) while `orders_by_state_month` is about orders, each with its user;
  the orphan query (0) is what makes the inner join safe: if it returned rows, the aggregate would silently drop them.
- `current_date` makes `age` change every day, so the table is stale a day after its creation: it needs a daily refresh,
  or `age` should be computed at query time from `birthdate`.
- The data files live under `s3://user-m-cuziat-ece/warehouse/gold/<table>-<uuid>/data/` (for example 1 file, 6 records,
  760 bytes for `top_products`). The location was decided by the catalog (Polaris, from the base location of the
  catalog), not by Trino.

### Refresh a gold table

`DELETE` then `INSERT` on `top_products` gives 3 snapshots: `append` (creation), `delete`, `append`. Between the two
statements the table has **0 rows**: a dashboard reading at that moment sees an empty table. A `MERGE` (or an
overwrite) is a single commit and a reader sees either the old or the new table. Time travel on the first snapshot
returns 6 products, as does the current table.

### Trino and Spark

- Trino only needs a client and an HTTP connection to run `top_products`; Spark needs a session (JVM and driver), the
  Iceberg runtime package, the S3 credentials and the Polaris credentials in its configuration, and runs as a job.
- A dashboard refreshed every minute on the gold layer: Trino (low latency, no start-up). A nightly transformation of
  10 TB of bronze files: Spark (distributed, fault tolerant, scalable shuffles). A data scientist exploring silver with a
  query: Trino.
- Trino fails the whole query if a worker is lost, unless fault-tolerant execution is enabled. This is acceptable for
  the queries of the lab, which last seconds, because a retry costs nothing; for a transformation of 10 TB running for
  hours, restarting from scratch after a failure would waste a lot of time and money.

## Lab 3: star schema

Same script and the same files.

- Tables: `dim_user` (50 rows), `dim_product` (6), `dim_date` (one row per day from 2020-01-01 to 2020-04-27, 118
  rows) and `fact_orders` (**2829** rows, like `silver.orders`).
- The top of the star query is `2020 FM brioche` (43 orders, 134 units), then `HI bread` 110, `VI bread` 110, `HI croissant`
  107... The aggregate `orders_by_state_month` rebuilt from the star returns the same rows as the table (for example
  AL 2020-01: 65 orders, 199 units, 1 buyer).
- Validation: the fact table has the same number of rows and the same quantity as the silver table, and no order is
  without a complete dimension row (`unmatched = 0`).

### Questions

- Are these checks enough? No. They count rows and keys but do not prove that each order is attached to the right
  user or product. For example, if the dimension had two rows for one `user_id`, the `LEFT JOIN` would duplicate
  the orders (caught by the row count), but a wrong `product` mapping, a quantity changed during the transformation
  compensated elsewhere, or orders attached to the wrong date (time zone) would pass. Additional checks: uniqueness of
  the natural key in every dimension, `not_null` on the keys, a comparison by group (units per product, per month,
  per user between silver and the star), and spot checks of rows.
- Grain: `fact_orders` has one row per order, `dim_date` one row per calendar day.
- If `silver.users` contains the same `user_id` twice, `dim_user` has two rows, the join duplicates the orders and the
  assertion on the number of rows of the fact table fails. An order with a `NULL` product is kept by the `LEFT JOIN`
  with a `NULL` `product_key`: the assertion on unmatched dimension rows fails.
- Surrogate keys from `row_number()`: if `dim_user` is rebuilt after a new user is added, the numbering can shift for
  the following users, so `user_key` values change; `fact_orders` must be rebuilt at the same time, otherwise its keys
  point to the wrong users. Stable keys (hash of the natural key or a persisted sequence) avoid this.
- `dim_user` overwrites the attributes: if a user moves, all the past orders show the new state. A dimension which keeps
  the history (slowly changing dimension type 2) needs validity dates (`valid_from`, `valid_to`) and a current flag, and
  the fact table must join on the version valid at the order date.
- `orders_by_state_month` answers one question fast (one scan of 50 rows), the star answers any question that
  combines the dimensions without a new table, but costs joins at query time and more tables to maintain.

### Exercises

1. Partitioned table (`partitioning = ARRAY['month(month)']`): the unpartitioned table has **1 file** (50 records), the
   partitioned one **4 files** (one per month: 17, 10, 13 and 10 records). On a filter on February, `EXPLAIN ANALYZE`
   shows **50 input rows** (80 % filtered) for the unpartitioned table and **10 input rows** for the partitioned one
   (partition pruning); the result is 2154 units for both.
2. `gold.cohorts` by decade of birth, for example 1910: 1 user, 47 orders, 3.19 units per order; 1930: 5 users, 198
   orders, 3.14; 1940: 4 users, 273 orders, 3.06. The totals are checked against the silver layer (50 users, 2829
   orders).
3. Insert, merge and time travel: after `INSERT INTO silver.orders` (one order) the silver table has 2830 orders and,
   after the `MERGE`, the gold table has 2830 orders and 8617 units; `FOR VERSION AS OF` the first snapshot of the
   gold table still gives 2829 orders and 8615 units. The `MERGE` is committed as an `overwrite` snapshot. The test
   order was then deleted and the gold table merged again: silver and gold are back at 2829 orders and 8615 units.
4. `ALTER TABLE gold.top_products EXECUTE optimize`: before and after, the table has 3 snapshots (`append`, `delete`,
   `append`) and 1 file of 6 records. Nothing changed because there is a single small file: `optimize` compacts small
   files into larger ones and only writes a new snapshot when it rewrites something.
5. Spark connected to Polaris: the aggregation of `orders_by_state_month` computed by Spark gives **50 rows, 2829
   orders, 8615 units**, and reading the gold table written by Trino from Spark gives the same numbers; the tables
   of the `gold` namespace (`dim_*`, `fact_orders`, `cohorts`...) are all visible in `polaris.gold`.

## Layout in the bucket

Each gold table has its own directory `warehouse/gold/<table>-<uuid>/` with `data/` and `metadata/`
(`00000-<uuid>.metadata.json` and the following versions, a `.stats` Puffin file, the manifest `*-m0.avro` and the
snapshot files). `orders_by_state_month` has three metadata versions (creation, the merge and the second merge).

## Not done

- The step that copies the `metadata/` files into Git (`aws s3 sync ... lab_trino`): the pod has no GitHub
  credentials. The layout above comes from `aws s3 ls`.
- Trino and Polaris are left running in the namespace (the teardown sections of the labs were not run).
