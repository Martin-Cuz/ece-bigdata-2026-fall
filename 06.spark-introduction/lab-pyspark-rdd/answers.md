# Lab PySpark RDD: answers

Run on the Onyxia `vscode-pyspark` service (Spark 4.1.1, `local[*]`, 2 cores). The whole lab is in
[rdd_lab.py](./rdd_lab.py), launched by [run-rdd.sh](./run-rdd.sh) with `spark-submit`: the datasets are downloaded from
`s3://user-m-cuziat-ece/bronze/`, because RDDs read files, not objects.

## Partitions

- `textFile()` gives **2 partitions** for each file. For this size the default is `minPartitions`, which is
  `min(defaultParallelism, 2)`; the Hadoop input format only splits a file further when it is larger than one block
  (128 MB).
- `repartition(8)` gives 8 partitions and shuffles all the data; `coalesce(1)` reduces the count without a shuffle by
  merging partitions that already exist (it can only decrease the number of partitions).

## Lazy evaluation

- `toDebugString()` shows the lineage `PythonRDD <- MapPartitionsRDD <- HadoopRDD (orders.csv)`, with no data read.
- `count()` returns **2829** orders in 0.627 s, a second `count()` 0.291 s (the JVM and the file system are warm).
  A second `count()` runs a **new job**: the RDD is not cached, so the file is read and filtered again; only
  `.cache()` placed on `orders_data` before the first action would avoid it.

## The multiline address

`textFile()` returns **101 lines** for 50 users: the header, then two physical lines per user (`...,"908 Jennifer
Squares` then `Robinsonshire, KY 01352",mail,birthdate`). Filtering the lines that start with a UUID keeps the
**50 records** of users, but the second line of each address is lost.

- Why does `spark.read.csv(multiLine=True)` not have the problem? The CSV parser tracks the quotes, so a line break
  inside a quoted field is part of the field. `textFile()` only splits on `\n`.
- A line-oriented reader is only safe on formats where a record never contains the record separator: no newline inside
  a value (JSON lines, or CSV where line breaks are escaped or forbidden).

## Manual parsing

Orders are parsed into dictionaries (2829 records). A malformed line (wrong number of fields or a quantity that is not
an integer) does not fail at read time: the job fails at the first action that touches it
(`Py4JJavaError`, `ValueError: too many values to unpack`). With an extra comma in `product`, the `split(",")` returns 6
fields and the cell fails the same way. The DataFrame API checks types when the data is read with an explicit schema
(or in `FAILFAST` mode), and reports bad rows according to the read mode (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`).

## reduceByKey vs groupByKey

Both give the same per-user quantity (`same result: True`). The shuffle volumes measured with the Spark REST API:

| Aggregation | Shuffle write | Shuffle read |
|---|---|---|
| `reduceByKey` | 2,513 bytes | 2,513 bytes |
| `groupByKey` | 6,230 bytes | 6,230 bytes |

`groupByKey` shuffles 2.5 times more data, even on 2829 rows. `reduceByKey` combines the values of each key inside each
partition first (map-side combine) and shuffles one partial sum per key and partition; `groupByKey` shuffles every
single value and builds the full list of values per key in memory afterwards. `groupByKey` is unavoidable when the
whole group of values is needed at once (median, a sorted list per key, collecting the values); `aggregateByKey` or
`combineByKey` cover the others.

## Manual deduplication

`reduceByKey(most_recent)` on `order_id` keeps one row per order: **2829 before and after**. The shuffle by key plays
the role of `partitionBy()` of the window function: it brings every row of the same `order_id` into the same partition.
The window function states the intent more directly (`PARTITION BY ... ORDER BY ... DESC`, keep the first) and the
optimizer can plan it; the `reduceByKey` is more efficient in memory (a combiner keeps a single row per key) but the
rule is hidden in a custom function.

## Join

`orders_kv.join(users_kv)` returns **2829** joined rows for 50 users, in 4 partitions. Spark can skip the shuffle when
both RDDs are already partitioned by the same partitioner and the same number of partitions (`partitionBy` before
`join`), or when one side is small enough to be broadcast (a manual broadcast of a dictionary in RDDs). With
`leftOuterJoin`, an order without matching user is kept with `None` instead of being dropped (see exercise 2).
`collect()` on a very large `enriched` would bring all the rows into the driver memory and crash it: use `take()`,
`count()` or write the result to storage.

## Exercises

1. `aggregateByKey((0, 0), seq, comb)` computes `(order_count, total_quantity)` per user in one pass, with no
   intermediate list; the average quantity per order is `total / count` (**2.99** overall, for example 2.71 to 3.57
   for the first five users).
2. With one fake order added, `leftOuterJoin` returns **1 order without user** (`fake-order`); the inner `join` still
   returns 2829 rows, so the unknown order is silently dropped there.
3. DataFrame version: `groupBy("user_uuid").sum("quantity")` takes 2 lines instead of 2 for the RDD but it is typed,
   readable and optimized: `explain()` shows an `AdaptiveSparkPlan` with a partial `HashAggregate`, an `Exchange`
   `hashpartitioning` and a final `HashAggregate` (**50 groups**), while `toDebugString()` only lists opaque Python RDDs
   that Spark cannot optimize.
4. Cache or not: `orders.count()` 0.193 s cached against 0.207 s not cached, `user_quantity.count()` 0.402 s against
   0.500 s. The files are tiny (330 KB), so the gain is small; it grows with the cost of the parsing step. In the
   Storage tab, only the cached RDD is listed (`is_cached` is `True` against `False`).
