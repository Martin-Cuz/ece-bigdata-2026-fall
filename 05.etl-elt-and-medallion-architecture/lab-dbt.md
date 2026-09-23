---
duration: 2h
category:
  - name: LAB
components:
  - name: DBT
  - name: DUCKDB
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: dbt (official documentation)
    url: https://docs.getdbt.com/docs/introduction
  - title: dbt-duckdb adapter
    url: https://github.com/duckdb/dbt-duckdb
  - title: dbt data tests (official documentation)
    url: https://docs.getdbt.com/docs/build/data-tests
  - title: dbt incremental models (official documentation)
    url: https://docs.getdbt.com/docs/build/incremental-models
  - title: dbt node selection syntax (official documentation)
    url: https://docs.getdbt.com/reference/node-selection/syntax
revisions:
  - date: 2026-09-17
    comment: Initial page
    author: pierre@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: Medallion architecture with dbt and DuckDB

## Objectives

- Create a dbt project using DuckDB as the execution engine
- Declare the CSV datasets stored on S3 as the sources of the bronze layer
- Build the silver layer: typed, cleaned and deduplicated models
- Validate the data with generic and singular tests, and investigate the failures
- Build the gold layer: an incremental fact table and an aggregate exported to S3 in Parquet
- Run the whole pipeline with `dbt build` and explore its lineage

## Prerequisites

- The `vscode-pyspark` Onyxia service and the project of the [uv lab](../03.object-storage/lab-1-uv.md)
- The `bronze/users.csv` and `bronze/orders.csv` objects uploaded at the end of the
  [S3 lab](../03.object-storage/lab-2-s3.md)
- The [DuckDB lab](../04.sql-analytics/lab-duckdb.md), in particular the S3 configuration with the
  `s3_onyxia_connection` secret

## Environment

Move into the project and set the environment variables used in the previous labs.

```bash
# Define the name of your repo/directory accordinly
GIT_REPO_NAME=<git-repo-name>
# Environment setup
cd /home/onyxia/work/$GIT_REPO_NAME
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
echo "$LAB_BUCKET_NAME"
#> user-gollum
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

dbt reads `LAB_BUCKET_NAME` when it compiles the models. The variable must be exported in every new terminal, or
appended to `~/.bashrc`.

## Installation

dbt Core is a Python package. Each database is supported by an adapter, a separate package which depends on `dbt-core`.
[dbt-duckdb](https://github.com/duckdb/dbt-duckdb) runs the models inside an embedded DuckDB database. Add it to the uv
project:

```bash
uv add dbt-duckdb
uv run dbt --version
#> Core:
#>   - installed: 1.12.5
#>   - latest:    1.12.5 - Up to date!
#>
#> Plugins:
#>   - duckdb: 1.11.0 - Up to date!
```

The versions may differ. dbt does not process the data itself: it compiles SQL statements and sends them to DuckDB,
which reads and writes the files on S3.

## dbt project

### Initialization

Create the project in a `lab_medallion` directory. The `--skip-profile-setup` flag disables the interactive prompts,
the connection is configured below.

```bash
uv run dbt init lab_medallion --skip-profile-setup
cd lab_medallion
rm -rf models/example
find . -type f | sort
#> ./analyses/.gitkeep
#> ./dbt_project.yml
#> ./.gitignore
#> ./macros/.gitkeep
#> ./README.md
#> ./seeds/.gitkeep
#> ./snapshots/.gitkeep
#> ./tests/.gitkeep
```

The remaining commands of the lab are executed from the `lab_medallion` directory. `uv run` looks for the
`pyproject.toml` file in the parent directories and uses the environment of the project.

- `dbt_project.yml`: the name of the project, the location of its files, and the configuration of its resources
- `models/`: the SQL models, and the YAML files documenting and testing them
- `seeds/`: small CSV files loaded as tables, used for reference data
- `tests/`: singular tests, SQL queries returning the rows which violate an assertion
- `macros/`: reusable Jinja functions
- `snapshots/`, `analyses/`: slowly changing dimensions and ad-hoc queries, not used in this lab

### Connection profile

The connection is defined in a profile. dbt looks for the `profiles.yml` file in the current directory first, then in
`~/.dbt/`. The profile of this lab contains no credential and is stored with the project:

```bash
cat > profiles.yml <<'YAML'
lab_medallion:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: lab_medallion.duckdb
      threads: 4
      settings:
        TimeZone: UTC
YAML
```

- `target`: the default output. A project typically defines several ones, such as `dev` and `prod`, selected with
  `--target`.
- `path`: the DuckDB database file storing the tables and views created by dbt. The data of the bronze layer stays on
  S3.
- `threads`: the number of models built in parallel.
- `settings`: DuckDB options applied to each connection.

The `s3_onyxia_connection` persistent secret, created by Onyxia, is loaded by the DuckDB library embedded in dbt, like
the CLI in the previous lab. Outside of Onyxia, secrets are declared in the profile with the `secrets` property.

Validate the configuration and the connection:

```bash
uv run dbt debug
#> ...
#> Configuration:
#>   profiles.yml file [OK found and valid]
#>   dbt_project.yml file [OK found and valid]
#> Required dependencies:
#>  - git [OK found]
#>
#> Connection:
#>   database: lab_medallion
#>   schema: main
#>   path: lab_medallion.duckdb
#> ...
#>   Connection test: [OK connection ok]
#>
#> All checks passed!
```

### Project configuration

Replace `dbt_project.yml`. The models are organized in one directory per layer, and each directory is configured with a
materialization and a schema:

```bash
cat > dbt_project.yml <<'YAML'
name: lab_medallion
version: "1.0.0"
profile: lab_medallion

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - target
  - dbt_packages

models:
  lab_medallion:
    silver:
      +materialized: view
      +schema: silver
    gold:
      +materialized: table
      +schema: gold

seeds:
  lab_medallion:
    +schema: reference
YAML
mkdir -p models/bronze models/silver models/gold
```

The `+` prefix marks a configuration inherited by all the resources of the directory, and each model can override it.
By default, dbt concatenates the schema of the target, `main`, and the custom schema: the silver models are created in
the `main_silver` schema. This prevents developers sharing a warehouse from overwriting each other's tables.

dbt generates files which are not source code. The `.gitignore` file created by `dbt init` is itself ignored by the
`.*` rule of the project, add the patterns to the `.gitignore` file at the root of the project instead:

```bash
cat >> ../.gitignore <<'INI'
target/
dbt_packages/
logs/
*.duckdb
INI
```

## Bronze layer

The bronze layer is not built by dbt: the raw files are loaded by the ingestion process, here the Kubernetes Job of the
S3 lab. dbt declares them as sources.

```bash
cat > models/bronze/sources.yml <<'YAML'
sources:
  - name: bronze
    description: Raw datasets uploaded to the S3 bucket by the dataset generators.
    config:
      meta:
        external_location: "read_csv('s3://{{ env_var('LAB_BUCKET_NAME') }}/bronze/{name}.csv', strict_mode = false)"
    tables:
      - name: users
        description: Users generated by `dataset-users`, one row per user.
      - name: orders
        description: Orders generated by `dataset-orders`, one row per order.
YAML
```

A source is referenced in a model with `{{ source('bronze', 'users') }}`. By default, dbt replaces it with the name of
a table in the database. The `external_location` property is specific to dbt-duckdb: the reference is replaced by a
`read_csv` call on the S3 object, `{name}` being substituted with the name of the table. The `env_var` Jinja function
reads the bucket name from the environment, which keeps it out of the code.

`dbt show` compiles and executes a query, and prints a preview of the result:

```bash
uv run dbt show -q --limit 3 \
  --inline "select uuid, user_uuid, date, quantity, product from {{ source('bronze', 'orders') }}"
#> | uuid                 | user_uuid            |                 date | quantity | product |
#> | -------------------- | -------------------- | -------------------- | -------- | ------- |
#> | 2339ba19-2563-4cc... | bdd640fb-0667-4ad... | 2020-01-01 00:02:... |        4 | drink   |
#> | b49e04cc-c243-49e... | bdd640fb-0667-4ad... | 2020-01-01 01:28:... |        5 | bread   |
#> | 41843b03-04dd-405... | bdd640fb-0667-4ad... | 2020-01-01 02:12:... |        2 | donut   |
```

Questions:

- The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months
  later?
- The source reads the whole file on every query. Which other format and layout would you choose if the orders were
  ingested every hour for years?

## Silver layer

The silver models clean the raw records, with one model per source table. By convention, they are prefixed with `stg_`
for staging.

### Users

The `address` column spans 2 lines: the street, then the city, the state and the zip code. Look at the raw addresses:
some of them, such as `DPO AP 09617`, are military addresses without a city.

```bash
uv run dbt show -q --limit 3 --inline "select address from {{ source('bronze', 'users') }}" --output json
#> {
#>   "show": [
#>     {
#>       "address": "908 Jennifer Squares\nRobinsonshire, KY 01352"
#>     },
#>     {
#>       "address": "Unit 6184 Box 9593\nDPO AP 09617"
#>     },
#>     {
#>       "address": "283 Steven Groves\nLake Mark, WI 07832"
#>     }
#>   ]
#> }
```

```bash
cat > models/silver/stg_users.sql <<'SQL'
with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
)

select
    user_id,
    username,
    name,
    sex,
    email,
    birthdate,
    street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed
SQL
```

The model is a `SELECT` statement split into common table expressions: one step reads the source, the next ones
transform it. The columns are renamed with consistent conventions (`uuid` becomes `user_id`, `mail` becomes `email`),
cast to their types, and standardized.

### Orders

```bash
cat > models/silver/stg_orders.sql <<'SQL'
with source as (
    select * from {{ source('bronze', 'orders') }}
),

typed as (
    select
        cast(uuid as uuid) as order_id,
        cast(user_uuid as uuid) as user_id,
        cast(date as timestamptz) as ordered_at,
        cast(quantity as integer) as quantity,
        lower(trim(product)) as product
    from source
)

select *
from typed
-- Keep a single row per order, the most recent one if the source delivers duplicates
qualify row_number() over (partition by order_id order by ordered_at desc) = 1
SQL
```

The generator does not produce duplicates, but ingestion processes do: retried extractions, overlapping incremental
loads. The deduplication makes the model robust to a replay of the bronze layer.

### Build

`dbt run` builds the models. The `--select` option restricts the execution to the models of the `silver` directory:

```bash
uv run dbt run --select silver
#> ...
#> 1 of 2 START sql view model main_silver.stg_orders ............................. [RUN]
#> 2 of 2 START sql view model main_silver.stg_users .............................. [RUN]
#> 1 of 2 OK created sql view model main_silver.stg_orders ........................ [OK in 0.07s]
#> 2 of 2 OK created sql view model main_silver.stg_users ......................... [OK in 0.07s]
#>
#> Finished running 2 view models in 0 hours 0 minutes and 0.13 seconds (0.13s).
#>
#> Completed successfully
#>
#> Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=2
```

dbt writes the compiled queries in `target/compiled/`, where the Jinja references are resolved, and the statements sent
to DuckDB in `target/run/`. Compare both versions of `stg_users.sql`:

```bash
cat target/compiled/lab_medallion/models/silver/stg_users.sql
cat target/run/lab_medallion/models/silver/stg_users.sql
```

Open the database with the DuckDB CLI in read-only mode to inspect the result. A DuckDB file is opened by a single
process in read-write mode: close the CLI before running dbt again.

```bash
duckdb -readonly lab_medallion.duckdb
```

```sql
SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY ALL;
-- ┌──────────────┬────────────┬────────────┐
-- │ table_schema │ table_name │ table_type │
-- │   varchar    │  varchar   │  varchar   │
-- ├──────────────┼────────────┼────────────┤
-- │ main_silver  │ stg_orders │ VIEW       │
-- │ main_silver  │ stg_users  │ VIEW       │
-- └──────────────┴────────────┴────────────┘
SELECT username, street, city, state, zip_code FROM main_silver.stg_users LIMIT 3;
-- ┌────────────────┬──────────────────────┬───────────────┬─────────┬──────────┐
-- │    username    │        street        │     city      │  state  │ zip_code │
-- │    varchar     │       varchar        │    varchar    │ varchar │ varchar  │
-- ├────────────────┼──────────────────────┼───────────────┼─────────┼──────────┤
-- │ garzaanthony   │ 908 Jennifer Squares │ Robinsonshire │ KY      │ 01352    │
-- │ blairamanda    │ Unit 6184 Box 9593   │ NULL          │ AP      │ 09617    │
-- │ elizabethmiles │ 283 Steven Groves    │ Lake Mark     │ WI      │ 07832    │
-- └────────────────┴──────────────────────┴───────────────┴─────────┴──────────┘
DESCRIBE main_silver.stg_orders;
-- ┌─────────────┬──────────────────────────┬─────────┬─────────┬─────────┬─────────┐
-- │ column_name │       column_type        │  null   │   key   │ default │  extra  │
-- │   varchar   │         varchar          │ varchar │ varchar │ varchar │ varchar │
-- ├─────────────┼──────────────────────────┼─────────┼─────────┼─────────┼─────────┤
-- │ order_id    │ UUID                     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ user_id     │ UUID                     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ordered_at  │ TIMESTAMP WITH TIME ZONE │ YES     │ NULL    │ NULL    │ NULL    │
-- │ quantity    │ INTEGER                  │ YES     │ NULL    │ NULL    │ NULL    │
-- │ product     │ VARCHAR                  │ YES     │ NULL    │ NULL    │ NULL    │
-- └─────────────┴──────────────────────────┴─────────┴─────────┴─────────┴─────────┘
.quit
```

Questions:

- The silver models are views. What happens when a view is queried, and when is it preferable to a table?
- `UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?

## Seeds

The gold layer groups the users by region. The mapping between a state code and its region is reference data: it
rarely changes and does not come from an operational system. dbt loads such small CSV files, versioned with the
project, as seeds.

```bash
cat > seeds/states.csv <<'CSV'
code,name,region
AL,Alabama,South
AK,Alaska,West
AZ,Arizona,West
AR,Arkansas,South
CA,California,West
CO,Colorado,West
CT,Connecticut,Northeast
DE,Delaware,South
DC,District of Columbia,South
FL,Florida,South
GA,Georgia,South
HI,Hawaii,West
ID,Idaho,West
IL,Illinois,Midwest
IN,Indiana,Midwest
IA,Iowa,Midwest
KS,Kansas,Midwest
KY,Kentucky,South
LA,Louisiana,South
ME,Maine,Northeast
MD,Maryland,South
MA,Massachusetts,Northeast
MI,Michigan,Midwest
MN,Minnesota,Midwest
MS,Mississippi,South
MO,Missouri,Midwest
MT,Montana,West
NE,Nebraska,Midwest
NV,Nevada,West
NH,New Hampshire,Northeast
NJ,New Jersey,Northeast
NM,New Mexico,West
NY,New York,Northeast
NC,North Carolina,South
ND,North Dakota,Midwest
OH,Ohio,Midwest
OK,Oklahoma,South
OR,Oregon,West
PA,Pennsylvania,Northeast
RI,Rhode Island,Northeast
SC,South Carolina,South
SD,South Dakota,Midwest
TN,Tennessee,South
TX,Texas,South
UT,Utah,West
VT,Vermont,Northeast
VA,Virginia,South
WA,Washington,West
WV,West Virginia,South
WI,Wisconsin,Midwest
WY,Wyoming,West
AS,American Samoa,Territories
GU,Guam,Territories
MP,Northern Mariana Islands,Territories
PR,Puerto Rico,Territories
VI,U.S. Virgin Islands,Territories
FM,Federated States of Micronesia,Freely associated states
MH,Marshall Islands,Freely associated states
PW,Palau,Freely associated states
CSV
uv run dbt seed
#> ...
#> 1 of 1 OK loaded seed file main_reference.states ............................... [INSERT 59 in 0.04s]
```

The seed is referenced in the models like any other model, with `{{ ref('states') }}`.

## Data tests

### Generic tests

Generic tests are declared in YAML, next to the models, along with their documentation. Each test compiles to a query
returning the rows which violate the assertion: the test passes when the query returns no row.

```bash
cat > models/silver/silver.yml <<'YAML'
models:
  - name: stg_users
    description: One row per user, typed, with the address split into its components.
    columns:
      - name: user_id
        description: Identifier of the user.
        data_tests:
          - unique
          - not_null
      - name: email
        data_tests:
          - unique
          - not_null
      - name: sex
        data_tests:
          - accepted_values:
              arguments:
                values: ["F", "M"]
      - name: state
        description: Two letters code of the state, territory or military region.
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('states')
                field: code

  - name: stg_orders
    description: One row per order, typed and deduplicated.
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: user_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_users')
                field: user_id
      - name: quantity
        data_tests:
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false
      - name: product
        data_tests:
          - accepted_values:
              arguments:
                values: ["bread", "brioche", "cookie", "croissant", "donut", "drink"]
YAML
```

- `unique` and `not_null` validate a primary key.
- `accepted_values` validates a closed list of values.
- `relationships` validates a foreign key: every value must exist in the referenced model.

Run the tests of the silver layer:

```bash
uv run dbt test --select silver
#> ...
#> 10 of 13 FAIL 6 relationships_stg_users_state__code__ref_states_ ............... [FAIL 6 in 0.05s]
#> ...
#> Completed with 1 error, 0 partial successes, and 0 warnings:
#>
#> [ERROR]: in test relationships_stg_users_state__code__ref_states_ (models/silver/silver.yml)
#>   Got 6 results, configured to fail if != 0
#>
#>   compiled code at target/compiled/lab_medallion/models/silver/silver.yml/relationships_stg_users_state__code__ref_states_.sql
#>
#> Done. PASS=12 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=13
```

### Investigate a failure

Display the compiled query of the failing test:

```bash
cat target/compiled/lab_medallion/models/silver/silver.yml/relationships_stg_users_state__code__ref_states_.sql
```

The `--store-failures` flag writes the rows returned by each test into a table of the `main_dbt_test__audit` schema:

```bash
uv run dbt test --select stg_users --store-failures
#> ...
#>   See test failures:
#>   -------------------------------------------------------------------------------------------------------
#>   select * from "lab_medallion"."main_dbt_test__audit"."relationships_stg_users_state__code__ref_states_"
#>   -------------------------------------------------------------------------------------------------------
duckdb -readonly lab_medallion.duckdb \
  -c 'SELECT * FROM main_dbt_test__audit.relationships_stg_users_state__code__ref_states_'
#> ┌────────────┐
#> │ from_field │
#> │  varchar   │
#> ├────────────┤
#> │ AE         │
#> │ AE         │
#> │ AE         │
#> │ AP         │
#> │ AP         │
#> │ AA         │
#> └────────────┘
```

`AA`, `AE` and `AP` are the codes of the military addresses: Armed Forces Americas, Europe and Pacific. The data is
valid, the reference data is incomplete. Complete the seed, load it again and run the tests:

```bash
cat >> seeds/states.csv <<'CSV'
AA,Armed Forces Americas,Military
AE,Armed Forces Europe,Military
AP,Armed Forces Pacific,Military
CSV
uv run dbt seed
#> 1 of 1 OK loaded seed file main_reference.states ............................... [INSERT 62 in 0.04s]
uv run dbt test --select silver
#> Done. PASS=13 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=13
```

A failing test does not always reveal invalid data: it reveals a wrong assumption, in the data or in the test.

### Singular tests

A singular test is a SQL query stored in the `tests/` directory, for assertions which do not fit a generic test. A user
cannot place an order before being born:

```bash
cat > tests/assert_orders_after_user_birth.sql <<'SQL'
-- A user cannot order before being born: the test fails if this query returns rows
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where o.ordered_at < u.birthdate
SQL
uv run dbt test --select silver
#> ...
#> 4 of 14 FAIL 288 assert_orders_after_user_birth ................................ [FAIL 288 in 0.13s]
#> ...
#> Done. PASS=13 WARN=0 ERROR=1 SKIP=0 NO-OP=0 REUSED=0 TOTAL=14
```

The number of failing rows depends on the day the dataset was generated. The orders are dated in 2020, while Faker
generates birthdates relative to the current date: some users are born years after their first order. This time, the
source data is wrong, and the bronze layer cannot be corrected.

Several strategies exist: reject the invalid records in the silver layer, replace the invalid birthdates with `NULL`,
ask the producer of the data to fix the generator, or accept the issue temporarily and monitor it. The last option is
implemented with the `severity` configuration, which turns the failure into a warning. Add the configuration at the top
of the test:

```bash
sed -i "1i {{ config(severity='warn') }}\n" tests/assert_orders_after_user_birth.sql
head -3 tests/assert_orders_after_user_birth.sql
#> {{ config(severity='warn') }}
#>
#> -- A user cannot order before being born: the test fails if this query returns rows
uv run dbt test --select silver
#> ...
#> 4 of 14 WARN 288 assert_orders_after_user_birth ................................ [WARN 288 in 0.07s]
#> ...
#> Done. PASS=13 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=14
```

Questions:

- Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the
  sales per region?
- The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which
  reference data would it require?

## Gold layer

### Fact table

`fct_orders` joins the orders with the users and the states: each row is an order, enriched with the attributes needed
by the analyses. It is materialized as an incremental table, the configuration in the model overrides the one of the
`gold` directory.

```bash
cat > models/gold/fct_orders.sql <<'SQL'
{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

select
    o.order_id,
    o.ordered_at,
    cast(o.ordered_at as date) as order_date,
    o.product,
    o.quantity,
    u.user_id,
    u.state,
    s.name as state_name,
    s.region
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
left join {{ ref('states') }} s on u.state = s.code
{% if is_incremental() %}
-- On incremental runs, only process the orders more recent than the latest order already loaded
where o.ordered_at > (select max(ordered_at) from {{ this }})
{% endif %}
SQL
uv run dbt run --select fct_orders
#> 1 of 1 OK created sql incremental model main_gold.fct_orders ................... [OK in 0.12s]
```

On the first run, the table does not exist: `is_incremental()` returns false and the whole query is executed. Run the
model a second time, and display the statements executed by dbt:

```bash
uv run dbt run --select fct_orders
cat target/compiled/lab_medallion/models/gold/fct_orders.sql
cat target/run/lab_medallion/models/gold/fct_orders.sql
```

The compiled query now contains the `where` clause, `{{ this }}` being replaced with the name of the existing table.
dbt stores the result in a temporary table, deletes the rows of `fct_orders` whose `order_id` is present in it, then
inserts the new rows. With `unique_key`, an order delivered twice replaces the previous version instead of being
duplicated.

The `--full-refresh` flag rebuilds the table from scratch, for example after a change of the logic:

```bash
uv run dbt run --select fct_orders --full-refresh
```

### Aggregate exported to S3

`sales_by_region_month` aggregates the orders by month, region and product, for a sales dashboard. The `external`
materialization of dbt-duckdb writes the result to a file instead of a table: the gold dataset is stored in the bucket
in Parquet, readable by any engine.

```bash
cat > models/gold/sales_by_region_month.sql <<'SQL'
{{
    config(
        materialized='external',
        location="s3://" ~ env_var('LAB_BUCKET_NAME') ~ "/gold/sales_by_region_month.parquet"
    )
}}

select
    strftime(order_date, '%Y-%m') as month,
    region,
    product,
    count(*) as orders,
    count(distinct user_id) as customers,
    cast(sum(quantity) as integer) as quantity
from {{ ref('fct_orders') }}
group by all
order by all
SQL
uv run dbt run --select sales_by_region_month
#> 1 of 1 OK created sql external model main_gold.sales_by_region_month ........... [OK in 0.06s]
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/gold/"
#> 2026-09-17 16:33:02       2369 sales_by_region_month.parquet
```

In the database, dbt-duckdb creates a view on the Parquet file, so that downstream models can reference it with `ref()`.
Query the file directly, without the database, and compare the result with the pivot of the DuckDB lab:

```bash
duckdb -c "
  SELECT product, sum(quantity) AS quantity, sum(orders) AS orders
  FROM 's3://$LAB_BUCKET_NAME/gold/sales_by_region_month.parquet'
  WHERE month = '2020-01'
  GROUP BY ALL
  ORDER BY quantity DESC
"
#> ┌───────────┬──────────┬────────┐
#> │  product  │ quantity │ orders │
#> │  varchar  │  int128  │ int128 │
#> ├───────────┼──────────┼────────┤
#> │ donut     │      438 │    143 │
#> │ brioche   │      431 │    133 │
#> │ drink     │      377 │    118 │
#> │ cookie    │      376 │    128 │
#> │ croissant │      360 │    116 │
#> │ bread     │      315 │    106 │
#> └───────────┴──────────┴────────┘
```

Questions:

- Why is the `quantity` column cast to an integer? Remove the cast, run the model, and look at the type in the Parquet
  file.
- The `customers` column counts distinct users. Can the dashboard sum it over several months? Over several products?
- Each run overwrites the Parquet file. What happens to a dashboard reading it during the write? Which technology,
  covered in the lakehouse modules, solves this issue?

## Pipeline

### Build

`dbt build` runs the seeds, the models and the tests in the order of the dependency graph. The tests of a model run
right after it, and the models depending on a failing test are skipped. Remove the database and build everything:

```bash
rm lab_medallion.duckdb
uv run dbt build
#> ...
#> 3 of 19 OK loaded seed file main_reference.states .............................. [INSERT 62 in 0.08s]
#> 2 of 19 OK created sql view model main_silver.stg_users ........................ [OK in 0.09s]
#> 1 of 19 OK created sql view model main_silver.stg_orders ....................... [OK in 0.14s]
#> ...
#> 13 of 19 WARN 288 assert_orders_after_user_birth ............................... [WARN 288 in 0.07s]
#> ...
#> 18 of 19 OK created sql incremental model main_gold.fct_orders ................. [OK in 0.07s]
#> 19 of 19 OK created sql external model main_gold.sales_by_region_month ......... [OK in 0.05s]
#>
#> Finished running 1 external model, 1 incremental model, 1 seed, 14 data tests, 2 view models in 0 hours 0 minutes and 0.57 seconds (0.57s).
#>
#> Completed with 1 warning:
#> ...
#> Done. PASS=18 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

To observe the protection offered by the tests, remove the 3 military rows from the seed and build again:

```bash
cp seeds/states.csv states.csv.bak
head -n -3 states.csv.bak > seeds/states.csv
uv run dbt build
#> ...
#> 8 of 19 FAIL 6 relationships_stg_users_state__code__ref_states_ ................ [FAIL 6 in 0.06s]
#> ...
#> 18 of 19 SKIP relation main_gold.fct_orders .................................... [SKIP]
#> 19 of 19 SKIP relation main_gold.sales_by_region_month ......................... [SKIP]
#> ...
#> Done. PASS=15 WARN=1 ERROR=1 SKIP=2 NO-OP=0 REUSED=0 TOTAL=19
mv states.csv.bak seeds/states.csv
uv run dbt build
```

The gold layer is not refreshed: the dashboard displays the data of the previous successful run, instead of sales
without region.

### Lineage

dbt knows the dependencies between the resources from the `ref()` and `source()` calls. The selection syntax navigates
the graph: `+model` selects a model and its ancestors, `model+` a model and its descendants.

```bash
uv run dbt ls -q --resource-type source seed model --select +sales_by_region_month
#> lab_medallion.gold.fct_orders
#> lab_medallion.gold.sales_by_region_month
#> lab_medallion.silver.stg_orders
#> lab_medallion.silver.stg_users
#> lab_medallion.states
#> source:lab_medallion.bronze.orders
#> source:lab_medallion.bronze.users
uv run dbt ls -q --resource-type model --output name --select stg_users+
#> fct_orders
#> sales_by_region_month
#> stg_users
```

Questions:

- Which command rebuilds `stg_orders` and all the models depending on it, and runs their tests?
- A column of `users.csv` is renamed by the producer. Which models are impacted, and at which step does `dbt build`
  fail?

### Documentation

`dbt docs generate` collects the descriptions of the YAML files and the schemas of the relations in `target/`. With
`--static`, the documentation website is written into a single HTML file:

```bash
uv run dbt docs generate --static
ls target/static_index.html
```

Download `target/static_index.html` from the VSCode explorer (right-click, "Download") and open it in your browser.
Browse the models, their columns and tests, and click the lineage button in the bottom right corner to display the
graph of the project.

### Commit

```bash
cd /home/onyxia/work/$GIT_REPO_NAME
git add .gitignore pyproject.toml uv.lock lab_medallion
git status --short
git commit -m "feat: medallion pipeline with dbt"
git push
```

Check that the `target/`, `logs/` directories and the `lab_medallion.duckdb` file are not staged.

## Exercises

1. Create a `dim_users` model in the gold layer, one row per user, with the name of their state, their region, the
   date of their first and last order, their number of orders and their total quantity. Declare `user_id` as unique and
   not null, and check that the number of rows equals the number of users.
2. Create a gold model answering a business question of your choice, such as the best-selling product per region, the
   evolution of the quantity week after week, or the share of each product per hour of the day. It must reference the
   upstream models with `ref()` only, apply at least 2 transformations beyond renaming columns (aggregation, derived
   metric, join, window function), and be documented and tested in a `models/gold/gold.yml` file.
3. Replace the warning of `assert_orders_after_user_birth` with a fix in the silver layer: the birthdate of a user is
   set to `NULL` when it is later than their first order. Add a `birthdate_is_valid` boolean column, and verify that
   the test passes.
4. Add a singular test verifying that the total quantity of `sales_by_region_month` equals the total quantity of
   `stg_orders`, so that no order is lost by the joins.
5. The silver layer reads the CSV files from S3 on every query. Materialize the silver models as `external` Parquet
   files in a `silver/` prefix of the bucket, and compare the execution time of `dbt build` before and after.

## Cleanup

Remove the objects of the gold layer. The objects of the bronze layer are kept for the next modules.

```bash
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/gold/" --recursive
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
#> 2026-09-14 11:02:10     331568 bronze/orders.csv
#> 2026-09-14 11:02:09       7351 bronze/users.csv
```

The `lab_medallion.duckdb` database file can be deleted as well: `dbt build` recreates all the models from the bronze
layer and the seeds.

---

_The content of this document, including all text, images, and associated materials, is the exclusive property of
Adaltas and is protected by applicable copyright laws. Unauthorized distribution, reproduction, or sharing of this
content, in whole or in part, is strictly prohibited without the express written consent of the author(s). Any violation
of this restriction may result in legal action and the imposition of penalties as prescribed by law._
