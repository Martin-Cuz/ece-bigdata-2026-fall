# Lab work index

Student: Martin Cuziat. Each chapter keeps the course text (`lab*.md`) and next to it the work done: the scripts that
were run, and an `answers.md` with the results and the answers to the questions.

| Chapter | Lab | Work |
|---|---|---|
| 01 Introduction to big data | Essential Linux commands | `01.introduction-to-big-data/data-platform-lab/` |
| 02 Kubernetes | Minikube and kubectl, ConfigMaps and Secrets | `02.containerization-and-kubernetes/lab-1/answers.md`, `lab-2/`, `lab-configmap-secret/` |
| 03 Object storage | Python project with uv | [`ece-2026-bigdata/`](./ece-2026-bigdata) (dataset generators) |
| 03 Object storage | S3 and Kubernetes upload Job | [`03.object-storage/lab-2-s3/`](./03.object-storage/lab-2-s3) |
| 04 SQL analytics | DuckDB | [`04.sql-analytics/lab-duckdb/`](./04.sql-analytics/lab-duckdb) |
| 05 ETL, ELT and medallion | dbt | [`05.etl-elt-and-medallion-architecture/lab-dbt/`](./05.etl-elt-and-medallion-architecture/lab-dbt) and [`ece-2026-bigdata/lab_medallion`](./ece-2026-bigdata/lab_medallion) |
| 06 Spark introduction | PySpark RDD | [`06.spark-introduction/lab-pyspark-rdd/`](./06.spark-introduction/lab-pyspark-rdd) |
| 07 Spark SQL and DataFrames | Bronze to silver with Spark | [`07.sparksql-and-dataframes/lab-pyspark-silver/`](./07.sparksql-and-dataframes/lab-pyspark-silver) |
| 08 Iceberg | Explore Iceberg tables | [`08.iceberg-introduction/lab-iceberg/`](./08.iceberg-introduction/lab-iceberg) |
| 09 Polaris | Deployment and governance | [`09.polaris/`](./09.polaris) |
| 10 Trino | | not done |

## How the labs were run

The labs 3 to 9 ran on the Onyxia platform (`vscode-pyspark` service, S3 bucket `user-m-cuziat-ece`): every
`answers.md` contains the observed results. Each lab folder has a `run-*.sh` script (and the Python or SQL files it
uses) which can be replayed from a terminal of the service; the scripts expect the AWS profile `default`, the
`KUBERNETES_NAMESPACE` variable and the bronze datasets in `s3://<bucket>/bronze/`.

Notes:

- Temporary credentials, the Polaris credentials and the generated data are never committed.
- `ece-2026-bigdata/uv.lock` was produced before `dbt-duckdb` was added: `uv sync` updates it.
- The Iceberg `metadata/` commit step of labs 7 and 9 was not done (no Git credentials in the pod).
