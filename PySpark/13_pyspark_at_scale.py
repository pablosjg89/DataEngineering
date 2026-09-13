"""
PySpark at Scale - Execution Plans, Caching/Persisting, Broadcast Joins

As data grows, optimizing PySpark jobs becomes essential for managing
performance, resource usage, and execution speed. Scaling PySpark isn't just
about faster results - it's about building workflows that are efficient,
maintainable, and capable of handling large datasets with ease.

Covers:
- Reading execution plans: .explain() in simple, extended, and formatted
  modes, and how to spot inefficiencies like shuffles (Exchange) in them
- Caching and persisting DataFrames with .cache()/.persist()/.unpersist(),
  so a reused DataFrame isn't recomputed from scratch for every action
- Persisting with different storage levels (MEMORY_ONLY, MEMORY_AND_DISK,
  DISK_ONLY, ...) for when a dataset doesn't comfortably fit in memory
- Broadcast joins: sending a small DataFrame to every executor with
  broadcast() to avoid an expensive shuffle join against a large one
- Best practices for optimizing PySpark jobs
"""

import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder \
    .appName("PySparkAtScale") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("PYSPARK AT SCALE")
print("=" * 80)

# ============================================================================
# SECTION 1: EXECUTION PLANS - .explain() in simple, extended, and formatted modes
# ============================================================================
print("\n" + "=" * 80)
print("1. EXECUTION PLANS - Reading .explain() to spot inefficiencies")
print("=" * 80)

employees_df = spark.createDataFrame([
    ("Alice", "Engineering", 95000),
    ("Bob", "Sales", 55000),
    ("Charlie", "Engineering", 70000),
    ("Diana", "Sales", 85000),
    ("Eve", "HR", 60000),
], ["Name", "Department", "Salary"])

query_df = employees_df.filter(col("Salary") > 60000) \
    .groupBy("Department") \
    .avg("Salary")

print("""
Whenever an operation runs on a DataFrame, Spark builds a logical plan, then
optimizes it into a physical plan describing how the cluster will actually
execute it. .explain() shows that process. Look for Exchange (a shuffle -
data moving between partitions/executors, often the most expensive part of
a job) and join strategies (SortMergeJoin/BroadcastHashJoin, see Section 4).
""")

print("--- explain() - default 'simple' mode: physical plan only ---")
query_df.explain()

print("\n--- explain(True) - 'extended' mode: parsed/analyzed/optimized logical")
print("--- plans, plus the physical plan - useful to see what the optimizer changed ---")
query_df.explain(True)

print("\n--- explain(mode='formatted') - the physical plan as a numbered tree, ---")
print("--- with each operator's details broken out separately and easier to scan ---")
query_df.explain(mode="formatted")

# ============================================================================
# SECTION 2: CACHING AND PERSISTING DATAFRAMES
# ============================================================================
print("\n" + "=" * 80)
print("2. CACHING AND PERSISTING DATAFRAMES - Avoid recomputing reused results")
print("=" * 80)

print("""
Every action on a DataFrame re-runs its transformations from scratch unless
that DataFrame is cached - reading melb_data.csv from disk and re-parsing it
for every single aggregation below would be wasteful, since it's the exact
same rows each time.
""")

dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Datasets", "melb_data.csv")
melb_df = spark.read.csv(dataset_path, header=True, inferSchema=True)

print(f"--- Before .cache(): is_cached = {melb_df.is_cached} ---")

melb_df.cache()
melb_df.count()  # an action is what actually materializes the cache - cache() alone is lazy
print(f"--- After .cache() + an action to materialize it: is_cached = {melb_df.is_cached} ---")
print(f"--- Storage level in use: {melb_df.storageLevel} ---")

print("\n--- Both aggregations below reuse the cached data instead of re-reading the CSV ---")
melb_df.groupBy("Type").count().show()
melb_df.groupBy("Regionname").avg("Price").show(5)

melb_df.unpersist()
print(f"--- After .unpersist(): is_cached = {melb_df.is_cached} ---")

# ============================================================================
# SECTION 3: PERSISTING WITH DIFFERENT STORAGE LEVELS
# ============================================================================
print("\n" + "=" * 80)
print("3. PERSISTING WITH DIFFERENT STORAGE LEVELS - persist(StorageLevel...)")
print("=" * 80)

print("""
.cache() defaults to a MEMORY_AND_DISK-style level (see its storageLevel in
Section 2) - .persist() lets you pick a different level explicitly:

  MEMORY_ONLY        - fastest, but recomputes any partition that doesn't fit
  MEMORY_AND_DISK     - what cache() uses: spill to disk instead of recomputing
  DISK_ONLY           - for datasets far too large to fit in memory at all
  MEMORY_AND_DISK_2   - like MEMORY_AND_DISK, replicated to 2 nodes for
                        fault tolerance, at the cost of extra network I/O

Choosing between them is a memory-vs-recompute-vs-replication tradeoff that
depends on cluster size and how critical the job is - this is hard to show
meaningfully on a single local machine, but matters a lot on a real cluster
with large, expensive-to-recompute datasets.
""")

melb_df.persist(StorageLevel.MEMORY_AND_DISK)
melb_df.count()
print(f"--- persist(MEMORY_AND_DISK): storageLevel = {melb_df.storageLevel} ---")
# Note this doesn't print identically to cache()'s storageLevel in Section 2
# (that one says "Deserialized", this says "Serialized") even though both
# requested the same nominal level - Spark's DataFrame cache manager stores
# cached data in its own serialized columnar format regardless of the
# deserialized flag on the StorageLevel you pass in.
melb_df.unpersist()

melb_df.persist(StorageLevel.DISK_ONLY)
melb_df.count()
print(f"--- persist(DISK_ONLY): storageLevel = {melb_df.storageLevel} ---")
melb_df.unpersist()
print(f"--- After .unpersist(): is_cached = {melb_df.is_cached} ---")

# ============================================================================
# SECTION 4: BROADCAST JOINS - Avoid a shuffle join against a small DataFrame
# ============================================================================
print("\n" + "=" * 80)
print("4. BROADCAST JOINS - Send a small DataFrame to every executor, skip the shuffle")
print("=" * 80)

print("""
Joining two large DataFrames normally needs a shuffle: matching keys have to
be moved onto the same partition on both sides (a SortMergeJoin). If one side
is small enough to fit in memory, broadcast() instead ships a full copy of it
to every executor, so the large side never needs to be shuffled at all.
""")

region_codes_df = spark.createDataFrame([
    ("Southern Metropolitan", "S-METRO"),
    ("Eastern Metropolitan", "E-METRO"),
    ("South-Eastern Metropolitan", "SE-METRO"),
    ("Northern Metropolitan", "N-METRO"),
    ("Western Metropolitan", "W-METRO"),
    ("Eastern Victoria", "E-VIC"),
    ("Northern Victoria", "N-VIC"),
    ("Western Victoria", "W-VIC"),
], ["Regionname", "RegionCode"])

# Force a plain shuffle join here (disabling Spark's own automatic broadcast)
# so the "before" plan below is honest, not silently already broadcasting
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

print("--- Plain join - SortMergeJoin, with an Exchange (shuffle) on BOTH sides ---")
melb_df.join(region_codes_df, on="Regionname").explain()

print("\n--- Same join, with broadcast() on the small side - BroadcastHashJoin, ---")
print("--- one BroadcastExchange instead, and NO shuffle of the large melb_df ---")
melb_df.join(broadcast(region_codes_df), on="Regionname").explain()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # restore the 10MB default

# ============================================================================
# SECTION 5: BEST PRACTICES FOR OPTIMIZING PYSPARK
# ============================================================================
print("\n" + "=" * 80)
print("5. BEST PRACTICES FOR OPTIMIZING PYSPARK JOBS")
print("=" * 80)

print("""
1. Favor targeted operations over whole-dataset shuffles - map()/filter()/
   select() transform each row independently with no shuffle; groupBy()/
   join()/distinct() usually require one. Push filters and column pruning
   before any shuffling operation so less data has to move.

2. Use broadcast joins for small datasets - as in Section 4, broadcast() a
   small lookup DataFrame to avoid an expensive shuffle join against a large
   one. Spark does this automatically under a size threshold
   (spark.sql.autoBroadcastJoinThreshold, 10MB by default), but an explicit
   hint helps once a DataFrame is borderline or the optimizer misjudges its size.

3. Avoid repeated actions on the same data - count(), show(), and collect()
   each trigger their own job. If a DataFrame is reused across several
   actions, cache()/persist() it first (Section 2) instead of recomputing
   it from scratch every time.

4. Read execution plans before running at scale - .explain() (Section 1)
   surfaces shuffles and join strategies up front, while the data is still
   small enough in development to fix cheaply.
""")

spark.stop()
