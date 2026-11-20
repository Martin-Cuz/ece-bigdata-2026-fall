# Lab Iceberg: answers

Run on the Onyxia `vscode-pyspark` service (Spark 4.1.1, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`, catalog `lab`
of type `hadoop` in `s3a://user-m-cuziat-ece/warehouse`). Script: [iceberg_lab.py](./iceberg_lab.py), run by
[run-iceberg.sh](./run-iceberg.sh).

## Create the table

`lab.silver.orders` is created from the silver Parquet of the Spark lab: **2829 orders, quantity 8615**. The catalog
lists the namespace `silver`. The tables are dropped (`DROP TABLE ... PURGE`) before the lab so the history starts at
a first snapshot.

## Inspect the files

Under `warehouse/silver/orders/`:

- `data/`: the Parquet files, which contain the rows (84,055 bytes for the 2829 rows of the first snapshot);
- `metadata/`: `vN.metadata.json` (the table: schema, partitioning, list of snapshots), the manifests `*-m0.avro` (the
  data files of a snapshot with their statistics), the manifest lists `snap-*.avro` and `version-hint.text`, which
  tells a reader which version is current.

- Which files contain the rows? The `data/*.parquet` files. Which files describe the table? Everything in `metadata/`.
- Why does an Iceberg table need more than the Parquet files? A set of Parquet files has no notion of which files
  belong to the table at a given time, of its schema history, of atomic commits or of file-level statistics for
  pruning. The metadata provides all that.

## Metadata tables

- After `createOrReplace` the table has **1 snapshot** (`overwrite`) and **1 data file** of 2829 records.
- The `snapshots` table gives what the files do not: the snapshot id, the operation (`overwrite`, `append`), the commit
  time, the parent snapshot and a summary of added or deleted records and files.

## Second snapshot

Appending 3 new orders (new `order_id` from `uuid()`) creates a **second snapshot** (`append`): the table has
**2832 rows** and **2 data files**. The original Parquet file (84,055 bytes) is still there: the append only adds a
new file (2,201 bytes) and a new manifest. Iceberg creates a snapshot instead of replacing the table so that readers
always see a consistent version, so that a failed commit leaves the previous state intact and so that older versions
stay available.

## Time travel

`VERSION AS OF <first snapshot>` returns **2829** rows while the current table has **2832**.

- What does time travel read? The metadata of the chosen snapshot, so exactly the data files that belonged to the table
  at that time.
- Why no complete copy? A snapshot is a list of existing files: the new snapshot reuses all the unchanged files and only
  adds the new ones.
- If the old metadata and data files were physically deleted (snapshot expiration, `remove_orphan_files`), time travel
  to that snapshot would fail: the history is only available as long as the files are kept.

## Schema evolution

`ALTER TABLE ... ADD COLUMN source STRING` is a metadata-only operation: the table now has the column `source`
(all existing rows return `NULL`), then one row is inserted with `source = 'spark'` (`NULL`: 2832, `spark`: 1).

- Did the existing Parquet files have to be rewritten? No.
- Where is the new schema recorded? In the new `vN.metadata.json`, which is a new version of the table metadata
  (the table directory has `v1` to `v4` after the lab), and each snapshot keeps the schema id it was written with.
- Interesting consequence: querying the **old snapshot** with `WHERE source IS NOT NULL` fails with
  `AnalysisException` (`UNRESOLVED_COLUMN`): time travel reads the schema of that snapshot, which has no `source`
  column. `SELECT *` on it returns the original 5 columns.
- Why is schema evolution a table-level operation? The files of a table may have different schemas over time; Iceberg
  tracks the columns by id and maps each file to the current schema when reading, which a bare Parquet directory
  cannot do (the readers would infer a schema from some file).

## Iceberg vs Parquet

- The directory `silver/spark/orders.parquet/` is only a collection of Parquet files (one part-file). The Iceberg
  `warehouse/silver/orders/` has table metadata, snapshots and manifests.
- Safely replacing a Parquet directory while another query reads it: write the new files in a new directory (or a new
  versioned path) and switch the readers to it afterwards (a pointer, a view or a rename), never overwrite in place.
- Iceberg makes the state visible atomically: the commit writes a new metadata file and then swaps the pointer to it;
  a reader uses the metadata file it opened at the start of its query.
- Why a catalog once several engines share the table? The pointer to the current metadata must be updated atomically
  and discovered by all the engines (Spark, Trino, Flink...); a catalog service stores that pointer, serializes the
  concurrent commits and holds the access control, which file-based catalogs on object storage cannot do.

## Exercises

1. `lab.silver.users` is created as an Iceberg table (50 users).
2. A batch of 2 users is appended: the snapshots list shows `overwrite` (50 records) then `append` (2 records).
3. Time travel on the users: **50** before the append, **52** after.
4. A column `country` is added and a new user is written with `country = 'FR'`: 52 users have `NULL` and 1 has `FR`.
5. Layout: each table has its own `data/` and `metadata/`. The orders table has 3 data files and 3 manifests
   (create, append, insert), the users table also has 3 data files (create, append, insert) and 3 manifests; both have
   `v1` to `v4` metadata files and a `version-hint.text`. The Parquet directory only has the data file.
6. Parquet is a file format (the data); Iceberg is a table format (the metadata that turns files into a table with
   snapshots, schema evolution and atomic commits); an Iceberg catalog is the service that knows where the current
   metadata of each table is (here a directory in the bucket, in the next lab a Polaris REST server).

## Cleanup

`warehouse/` was removed from the bucket at the end of the lab (`aws s3 rm ... --recursive`).
