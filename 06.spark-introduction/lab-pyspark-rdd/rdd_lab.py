import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


def section(title):
    print(f"\n### {title}", flush=True)


section("partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("default partitions users/orders:", users_lines.getNumPartitions(), orders_lines.getNumPartitions())
print("default parallelism:", sc.defaultParallelism, "defaultMinPartitions:", sc.defaultMinPartitions)
orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8):", orders_repartitioned.getNumPartitions())
print("after coalesce(1):", orders_repartitioned.coalesce(1).getNumPartitions())

section("lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
t = time.perf_counter()
print("count:", orders_data.count(), f"{time.perf_counter() - t:.3f}s")
t = time.perf_counter()
print("count again:", orders_data.count(), f"{time.perf_counter() - t:.3f}s")

section("multiline address")
for line in users_lines.take(6):
    print(repr(line[:100]))
print("textFile lines:", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("user records:", users_records.count())


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


section("manual parsing of orders")
orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)
print("parsed orders:", orders_rdd.count())
bad = sc.parallelize(["a,b,c,d,e,f", "x,y,2020-01-01,notanumber,z"]).map(parse_order)
try:
    bad.collect()
except Exception as e:
    print("malformed line fails at the action:", type(e).__name__)

section("reduceByKey vs groupByKey")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))
user_quantity_grouped = (
    orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
)
print(user_quantity_grouped.take(5))
print("same result:", dict(user_quantity.collect()) == dict(user_quantity_grouped.collect()))
try:
    app = sc.applicationId
    stages = json.load(urllib.request.urlopen(f"http://localhost:4040/api/v1/applications/{app}/stages"))
    for s in sorted(stages, key=lambda s: s["stageId"]):
        if s.get("shuffleWriteBytes", 0) or s.get("shuffleReadBytes", 0):
            print(
                "stage", s["stageId"], s["name"][:40],
                "shuffleWrite", s["shuffleWriteBytes"], "shuffleRead", s["shuffleReadBytes"],
                "records w/r", s["shuffleWriteRecords"], s["shuffleReadRecords"],
            )
except Exception as e:
    print("spark ui not reachable:", type(e).__name__)

section("manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("orders before/after dedup:", orders_rdd.count(), orders_dedup.count())

section("join")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
for user_id, (order, username) in enriched.take(3):
    print(user_id, order["order_id"], order["product"], username)
print("joined rows:", enriched.count(), "users:", users_kv.count())
print("join partitions:", enriched.getNumPartitions())

section("exercise 1: aggregateByKey")
stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
average = stats.mapValues(lambda s: round(s[1] / s[0], 2))
for row in average.take(5):
    print(row)
print("users:", stats.count(), "global average:", round(sum(v for _, v in average.collect()) / stats.count(), 2))

section("exercise 2: leftOuterJoin with an unknown user")
fake_order = {"order_id": "fake-order", "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff", "date": "2020-01-01", "quantity": 1, "product": "bread"}
orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))
left = orders_with_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
no_user = left.filter(lambda kv: kv[1][1] is None)
print("orders without user:", no_user.count(), [kv[1][0]["order_id"] for kv in no_user.take(3)])
print("join (inner) rows:", orders_with_fake.map(lambda o: (o["user_id"], o)).join(users_kv).count())

section("exercise 3: DataFrame API")
df = spark.read.csv("orders.csv", header=True, inferSchema=True)
df_total = df.groupBy("user_uuid").sum("quantity")
df_total.explain()
print("DataFrame groupBy rows:", df_total.count())
print(user_quantity.toDebugString().decode())

section("exercise 4: cache or not")
orders_nocache = orders_data.map(parse_order)
for label, rdd in [("cached", orders_rdd), ("not cached", orders_nocache)]:
    uq = rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    t = time.perf_counter()
    rdd.count()
    t1 = time.perf_counter() - t
    t = time.perf_counter()
    uq.count()
    t2 = time.perf_counter() - t
    print(label, f"orders.count() {t1:.3f}s", f"user_quantity.count() {t2:.3f}s", "storage level:", rdd.getStorageLevel())
print("cached RDDs:", [(r.name(), r.memSize) for r in sc._jsc.sc().getRDDStorageInfo()] if False else len(sc._jsc.sc().getPersistentRDDs()))

spark.stop()
print("\n### done")
