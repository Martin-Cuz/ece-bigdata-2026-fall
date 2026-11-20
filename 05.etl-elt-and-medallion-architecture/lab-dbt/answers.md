# Lab dbt: answers

Run on the Onyxia `vscode-pyspark` service. The dbt project is `ece-2026-bigdata/lab_medallion`; the run is scripted in
[run-dbt.sh](./run-dbt.sh). The sources read `s3://user-m-cuziat-ece/bronze/{users,orders}.csv` through DuckDB.

## Project

- `dbt-duckdb` is a dependency of the uv project. `profiles.yml` has no credential (the S3 access comes from the
  persistent DuckDB secret) and `dbt debug` passes.
- `models/bronze/sources.yml` declares the two bronze sources with `external_location` (`read_csv` on the object). The
  location is read from the `LAB_BUCKET_NAME` environment variable.
- `stg_orders` types the columns and deduplicates on `order_id` (`QUALIFY row_number() ... = 1`): **2829 rows before and
  after**, the generator does not produce duplicates.
- `stg_users` splits the two-line address into `street`, `city`, `state` and `zip_code`. Military addresses such as
  `DPO AP 09617` have no city (`NULL`), and their state is the region code (`AA`, `AE`, `AP`).
- Silver models are views in the `main_silver` schema; `dbt ls` shows the lineage `source:bronze.orders -> stg_orders`.
- `dbt docs generate --static` writes `target/static_index.html` (2.5 MB).

## Tests

`dbt build` runs 2 seeds, 2 models and 20 tests: **PASS=23, WARN=1, ERROR=0**.

- Generic tests: `unique` / `not_null` on the keys, `accepted_values` on `sex`, `quantity` and `product`,
  `relationships` from `stg_orders.user_id` to `stg_users` and from `stg_users.state` to the `states` seed.
- A failing test, investigated: the first run of `relationships` on `state` failed for **6 users**. Looking at their
  address showed military regions (`AA`, `AE`, `AP`) missing from the `states` seed; adding them fixed it. This is the
  workflow of the lab: read the compiled query in `target/compiled/`, or run `dbt test --store-failures` to get the
  failing rows in the `main_dbt_test__audit` schema.
- Singular tests: `assert_orders_have_users` (0 rows), `assert_positive_quantity` (0 rows),
  `assert_username_normalized` (0 rows) and `assert_orders_after_user_birth`.
- Birth dates (exercise 3): Faker generates birth dates relative to the current date while the orders are dated 2020, so
  some users are born after their first order. **5 of the 50 users** have an invalid birth date. In `stg_users` the
  birth date of these users is replaced by `NULL` and `birthdate_is_valid` is `false`, so the singular test passes.
- ZIP codes (exercise 4): `assert_zip_matches_state` compares the zip code with the range of its state in the
  `state_zip_ranges` seed. **43 of the 50 users** have a zip code outside the range of their state: the test is set to
  `severity: warn`, it is a known defect of the generated data (the generator draws state and zip code independently).

### Questions

- Which strategy for a dashboard displaying the age of customers, and for one displaying sales per region? For the age
  dashboard, set the invalid birth date to `NULL` (as done here) and exclude it from the averages, with the
  `birthdate_is_valid` flag to show how many were dropped: a false age is worse than a missing one. For sales per
  region the birth date does not matter, so the warning is enough and the orders must not be rejected.
- Which test would detect state / zip code mismatches, and which reference data does it need? A test that joins the
  user with a state-to-zip-range reference table, as `assert_zip_matches_state` does. The reference data should come
  from an authoritative source (USPS), be versioned as a seed (small and slowly changing) or loaded as a source table,
  and the validation belongs in the silver layer, where the data is checked before it is published.

## Bronze layer questions

- The bronze files are left untouched: what is the benefit when a bug is found six months later? The raw data is the
  source of truth: the silver layer can be rebuilt from it after the fix, with no loss and no need to re-ingest.
- If orders were ingested every hour for years, which format and layout? Parquet (columnar, compressed, typed)
  partitioned by date (`orders/date=YYYY-MM-DD/`), so that queries read only the useful partitions.
- The silver models are views: what happens when a view is queried, and when is it preferable to a table? The query
  of the view is executed again at each use, on the current data. A view costs no storage and is always up to date,
  which suits cheap transformations on small or rarely queried data; a table is better for expensive transformations
  or data queried often.
- `UUID` values use 16 bytes; the `VARCHAR` form is 36 bytes of text (plus an overhead).

## Parquet export

`stg_users` (50 rows, 8,123 bytes) and `stg_orders` (2829 rows, 72,538 bytes) are exported to
`s3://user-m-cuziat-ece/silver/dbt/`. A query on the Parquet file returns the same counts as the view (`2829` and
`2829`; `F` 30, `M` 20). A **view** is logical materialization (the query is stored and run every time); a **Parquet
file** is physical materialization (the result is stored once and read).

## Exercise 5: materialization

With `SILVER_MATERIALIZED=table` the models are physical tables: the DuckDB file grows from **798,720 bytes** (views
only) to **1,323,008 bytes**, and the full `dbt build` takes 3.9 s. Views store no data, run the transformation at
every query and always re-read the CSV files if the source changes; tables store the data, run the transformation once
and avoid re-reading the sources, but must be rebuilt to see a change in the CSV. The project was restored to views.

## Cleanup

The silver objects in S3 were removed; the bronze layer is kept.
