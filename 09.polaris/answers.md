# Polaris labs: answers

Run on the Onyxia `vscode-pyspark` service (limits 2 CPU and 2500 Mi as the labs require), Polaris 1.8.0 deployed with
Helm in the namespace `user-m-cuziat-ece`.

## Lab 1: deployment

Files: [lab-1-deployment/polaris/deployment](./lab-1-deployment/polaris/deployment) (the chart of the lab) and
[run-deploy.sh](./lab-1-deployment/run-deploy.sh) (ConfigMap, Secrets, `helm upgrade --install`, credentials, checks).

- Deployed pods: `polaris`, `polaris-postgresql-0` and the Job `polaris-bootstrap` (Completed).
- The Polaris pod stayed `Pending` for several minutes: `0/9 nodes are available: 3 Insufficient cpu`. The CPU
  **requests** of the pod (100m) and of its init container (50m) could not be satisfied on the busy cluster. The CPU
  requests were lowered to 10m (limits unchanged) in `values.yaml`, and the bootstrap Job was recreated with the same
  request; the pod then started. A request is only what the scheduler reserves, not a limit on the usage.
- The bootstrap Job log ends with `Polaris RBAC bootstrap completed`: `dev -> data_engineer -> catalog_contributor`
  and `analyst -> data_analyst -> catalog_reader`.
- Admin API checked with a root token: catalog `user-m-cuziat-ece`, principals `root`, `analyst`, `dev`, principal
  roles `data_analyst`, `service_admin`, `data_engineer`, catalog roles `catalog_reader`, `catalog_contributor`,
  `catalog_admin`.
- The credentials of `dev` and `analyst` are only returned at creation: they are kept in
  `/home/onyxia/work/.polaris-credentials.env` (outside the Git repository) and are not committed.

## Lab 2: governance

Files: [lab-2/polaris_lab.py](./lab-2/polaris_lab.py) and [lab-2/run-polaris.sh](./lab-2/run-polaris.sh).

### SparkSession with the Polaris catalog

`SHOW NAMESPACES IN lab` lists `silver` (the Hadoop catalog reads the directories of the bucket) and
`SHOW NAMESPACES IN polaris` is empty: Polaris does not know any table yet.

- Which component answered? The first one is Spark itself reading `s3a://.../warehouse` through the Hadoop catalog,
  with no server; the second one is the Polaris REST server (`http://polaris:8181/api/catalog`). The results differ
  because they read two different sources of truth.
- Where is the credential used? In the request to Polaris: the `dev` credential (`client_id:client_secret`) is exchanged
  for an OAuth token and sent as a bearer token on every REST call. It is not used for the bucket: the data files are
  read and written with the S3 keys of the session (no credential vending on this storage).

### Register the silver table

`register_table` on the current metadata file returns `current_snapshot_id 1333229184899724736`, **2829 records**,
1 data file, and `SELECT count(*)` through `polaris.silver.orders` gives **2829**.

- What did it write? In the bucket, nothing: `warehouse/silver/orders/` has the same **5 objects** before and after
  (1 data file, 1 manifest, 1 snapshot file, `v1.metadata.json`, `version-hint.text`). Polaris only stores, in its
  PostgreSQL database, the table name and a pointer to the metadata file.
- Why is it dangerous to keep writing through `lab`? The two catalogs have independent pointers. A commit through
  `lab` writes a new metadata version and moves the pointer of the Hadoop catalog, while Polaris still points to the
  old version: readers of Polaris miss the new data, and a later commit through Polaris starts from the old state and
  can overwrite the other commit.

### Create tables as dev

`polaris.silver.users` is created from `lab.silver.users` (**50 users**).

- Where did Polaris store the files? Under `warehouse/silver/users/` (`data/` and `metadata/`): the location of a new
  table is decided by the catalog from its `default-base-location` (`s3://user-m-cuziat-ece/warehouse/`), inside the
  `allowedLocations` of the catalog.
- Registered or created through Polaris? A registered table keeps its data files where the other catalog wrote them and
  Polaris only points to them; for a table created through Polaris, Polaris chooses the location, Spark writes the data
  files there and the table has no `version-hint.text` (the metadata files are named `00000-<uuid>.metadata.json`
  and the pointer is in Polaris, not in the bucket).

### Schema change as dev

`ALTER TABLE polaris.silver.users ADD COLUMN country STRING` works and `DESCRIBE` shows `country`.

- Which metadata file records the change? A new metadata file of the users table
  (`00001-<uuid>.metadata.json`), written by the commit; it is not the file `register_table` pointed to (that one
  belongs to the orders table).
- Which privilege does Polaris check? The privileges to write the table metadata, which the `data_engineer` receives
  through `CATALOG_MANAGE_CONTENT` of `catalog_contributor`; it is the same check that rejects the analyst (below).

### Read as the analyst

- `SHOW TABLES IN polaris.silver` lists `users` and `orders`, and `GROUP BY product` works (bread 458, brioche 468,
  cookie 466, croissant 491, donut 462, drink 484).
- `INSERT INTO polaris.silver.orders ...` fails with
  `ForbiddenException: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via
  '[data_analyst, catalog_reader]' is not authorized for op ADD_TA...` (the operation name is cut in the log), and
  `ALTER TABLE ... ADD COLUMN phone` fails the same way.
- Which privilege is missing for the `INSERT`? The write privilege on the table (`TABLE_WRITE_DATA`, part of
  `CATALOG_MANAGE_CONTENT`). `catalog_reader` only provides `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and
  `CATALOG_READ_PROPERTIES`.
- Did the failed `INSERT` create a snapshot? No: as `dev`, `silver.orders` still has its single `overwrite` snapshot and
  2829 rows. Polaris refuses the commit before any metadata is written.

### Exercises

1. Drop the `country` column: the users table has the same snapshot before and after (one `overwrite`) and 3 metadata
   files (creation, add column, drop column). It is a metadata operation: the columns are tracked by id in the table
   metadata and the Parquet data files are not rewritten.
2. A catalog role `silver_reader` with `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`,
   `TABLE_READ_PROPERTIES` and `TABLE_READ_DATA` on the namespace `silver` only, a principal role
   `silver_reader_role` and a principal `reader` were created with the management API (all calls return `201`).
   A `gold` namespace with the table `gold.product_summary` (6 rows) was created as `dev`. The `reader` principal:

   | Operation | Result |
   |---|---|
   | list the namespaces of the catalog | forbidden (needs the listing privilege at catalog level) |
   | list the tables of `silver` | allowed (`orders`) |
   | `SELECT count(*)` on `silver.orders` | allowed (**2829**) |
   | list the tables of `gold` | forbidden |
   | read `gold.product_summary` | forbidden |
   | `INSERT` into `silver.orders` | forbidden |

   The role, principal role and principal were deleted afterwards (`204`).

### Cleanup

`polaris.silver.users`, `polaris.gold.product_summary` and the `gold` namespace were dropped as `dev`.
`silver.orders` stays registered in Polaris, as the labs ask, and Polaris is kept running.

## Not done

- The step that commits the Iceberg `metadata/` files to Git: the pod has no GitHub credentials, and the metadata is
  only inspected in S3 (listings above).
- The Trino labs (chapter 10) were not part of this work.
