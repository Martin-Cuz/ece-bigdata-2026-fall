import os
import time
from pathlib import Path

import duckdb

root = Path(__file__).parents[1].parent / "ece-2026-bigdata" / "data" / "large"
csv, pq = root / "orders.csv", root / "orders.parquet"
con = duckdb.connect()
con.execute("SET TimeZone = 'UTC'")

t = time.time()
con.execute(f"COPY (FROM read_csv('{csv.as_posix()}', strict_mode=false)) TO '{pq.as_posix()}' (FORMAT parquet)")
print(f"convert csv->parquet: {time.time() - t:.2f}s")
print("rows:", con.execute(f"SELECT count(*) FROM read_parquet('{pq.as_posix()}')").fetchone()[0])
print(f"csv size: {os.path.getsize(csv) / 1e6:.1f} MB, parquet size: {os.path.getsize(pq) / 1e6:.1f} MB")

queries = {
    "csv group by": f"SELECT product, sum(quantity) FROM read_csv('{csv.as_posix()}', strict_mode=false) GROUP BY product",
    "parquet group by": f"SELECT product, sum(quantity) FROM read_parquet('{pq.as_posix()}') GROUP BY product",
    "parquet filter date>=2100": f"SELECT count(*) FROM read_parquet('{pq.as_posix()}') WHERE date >= '2100-01-01'",
}
for name, q in queries.items():
    con.execute("SET enable_external_file_cache = false")
    times = []
    for _ in range(3):
        t = time.time()
        con.execute(q).fetchall()
        times.append(time.time() - t)
    print(f"{name}: best of 3 = {min(times):.3f}s")

print(
    con.execute(
        f"SELECT row_group_id, row_group_num_rows, stats_min, stats_max FROM parquet_metadata('{pq.as_posix()}') WHERE path_in_schema = 'date'"
    ).fetchall()
)
