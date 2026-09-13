"""
RDDs (Resilient Distributed Datasets)

RDDs are Spark's original, low-level distributed data structure - an
immutable, partitioned collection of Python objects processed in parallel
across the cluster. DataFrames are built on top of RDDs and add a schema
plus the Catalyst/Tungsten query optimizer, which is why DataFrames are
almost always preferred today.

DataFrames vs. RDDs:
- RDDs offer a low-level interface, giving maximum flexibility - you can
  manipulate data at a granular level, but that flexibility costs more lines
  of code for even moderately complex operations.
- RDDs preserve Python data types across operations, but lack the
  schema-aware optimizations DataFrames get from Catalyst/Tungsten, so
  operations on structured data are both less efficient and harder to
  express.
- RDDs scale to large datasets, but aren't as optimized for analytics
  workloads as DataFrames are.
- DataFrames are optimized for ease of use: a high-level abstraction that
  encapsulates complex computations, so the same goal takes less code and
  fewer opportunities for mistakes.
- DataFrames' standout feature is SQL-like functionality - complex
  transformations and analyses expressed in a few lines via SQL syntax or
  the DataFrame API.
- DataFrames carry built-in schema awareness (column names and data types),
  just like a structured table in SQL; RDDs carry none of that - just
  whatever Python objects you put into them.

Reach for RDDs when you need:
- Fine-grained control over partitioning or low-level transformations
- To process unstructured data that doesn't fit a DataFrame schema well
- To call an API that only exists at the RDD level (mapPartitions, custom
  partitioners, etc.)

Two kinds of RDD operations:
- Transformations (map, filter, flatMap, ...) are lazy - they just build up
  a computation plan and return a new RDD
- Actions (collect, count, reduce, ...) trigger actual execution and return
  a result to the driver
"""

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDDs") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 80)
print("PYSPARK RDDs (RESILIENT DISTRIBUTED DATASETS)")
print("=" * 80)

# ============================================================================
# SECTION 1: CREATING RDDs
# ============================================================================
print("\n" + "=" * 80)
print("1. CREATING RDDs - parallelize() distributes a local collection")
print("=" * 80)

numbers_rdd = sc.parallelize(range(1, 11))

print("\n--- parallelize() - Create an RDD from a local Python collection ---")
print(f"Numbers RDD contents: {numbers_rdd.collect()}")
print(f"Number of partitions: {numbers_rdd.getNumPartitions()}")

# ============================================================================
# SECTION 2: TRANSFORMATIONS - Lazy, return a new RDD
# ============================================================================
print("\n" + "=" * 80)
print("2. TRANSFORMATIONS - map(), filter(), flatMap(), distinct()")
print("=" * 80)

# map() - apply a function to every element
print("\n--- map() - Square every number ---")
squared_rdd = numbers_rdd.map(lambda n: n * n)
print(f"Squares: {squared_rdd.collect()}")

# filter() - keep elements matching a predicate
print("\n--- filter() - Keep only even numbers ---")
evens_rdd = numbers_rdd.filter(lambda n: n % 2 == 0)
print(f"Evens: {evens_rdd.collect()}")

# flatMap() - map each element to zero or more outputs, then flatten
print("\n--- flatMap() - Split sentences into individual words ---")
sentences_rdd = sc.parallelize([
    "Spark makes big data simple",
    "RDDs are the foundation of Spark",
])
words_rdd = sentences_rdd.flatMap(lambda sentence: sentence.lower().split())
print(f"Words: {words_rdd.collect()}")

# distinct() - remove duplicate elements
print("\n--- distinct() - Unique words across all sentences ---")
print(f"Distinct words: {sorted(words_rdd.distinct().collect())}")

# ============================================================================
# SECTION 3: ACTIONS - Trigger execution, return a result to the driver
# ============================================================================
print("\n" + "=" * 80)
print("3. ACTIONS - collect(), count(), first(), take(), reduce()")
print("=" * 80)

print("\n--- count() - Number of elements ---")
print(f"Word count: {words_rdd.count()}")

print("\n--- first() / take(n) - Grab one or a few elements without a full scan ---")
print(f"First word: {words_rdd.first()}")
print(f"First 3 words: {words_rdd.take(3)}")

print("\n--- reduce() - Combine all elements into a single value ---")
total = numbers_rdd.reduce(lambda a, b: a + b)
print(f"Sum of 1..10 via reduce(): {total}")

# ============================================================================
# SECTION 4: PAIR RDDs - Key-value operations
# ============================================================================
print("\n" + "=" * 80)
print("4. PAIR RDDs - reduceByKey(), groupByKey(), mapValues(), sortByKey()")
print("=" * 80)

employees_rdd = sc.parallelize([
    ("Engineering", 80000),
    ("Sales", 65000),
    ("Engineering", 75000),
    ("Sales", 70000),
    ("HR", 55000),
])

# reduceByKey() - combine values sharing a key using an associative function
print("\n--- reduceByKey() - Total salary per department ---")
total_by_dept = employees_rdd.reduceByKey(lambda a, b: a + b)
print(f"Total salary by department: {sorted(total_by_dept.collect())}")

# groupByKey() - collect all values sharing a key into a list
# (prefer reduceByKey/aggregateByKey when combining values - groupByKey ships
# every value across the network before combining, which doesn't scale as well)
print("\n--- groupByKey() - All salaries grouped per department ---")
grouped_by_dept = employees_rdd.groupByKey().mapValues(list)
print(f"Salaries by department: {sorted(grouped_by_dept.collect())}")

# mapValues() - transform only the value, keeping the key unchanged
print("\n--- mapValues() - Apply a 10% raise to every salary ---")
raised_rdd = employees_rdd.mapValues(lambda salary: round(salary * 1.10))
print(f"Salaries after raise: {sorted(raised_rdd.collect())}")

# sortByKey() - order pairs by key
print("\n--- sortByKey() - Departments in alphabetical order ---")
print(f"Sorted by department: {employees_rdd.sortByKey().collect()}")

# ============================================================================
# SECTION 5: SET-LIKE OPERATIONS - union(), intersection(), subtract()
# ============================================================================
print("\n" + "=" * 80)
print("5. SET-LIKE OPERATIONS - union(), intersection(), subtract()")
print("=" * 80)

team_a = sc.parallelize(["Alice", "Bob", "Charlie"])
team_b = sc.parallelize(["Bob", "Charlie", "Diana"])

print("\n--- union() - Combine two RDDs (keeps duplicates) ---")
print(f"Team A + Team B: {sorted(team_a.union(team_b).collect())}")

print("\n--- intersection() - Elements present in both RDDs ---")
print(f"On both teams: {sorted(team_a.intersection(team_b).collect())}")

print("\n--- subtract() - Elements in the first RDD but not the second ---")
print(f"Only on Team A: {sorted(team_a.subtract(team_b).collect())}")

# ============================================================================
# SECTION 6: PERSISTENCE - cache() / persist() / unpersist()
# ============================================================================
print("\n" + "=" * 80)
print("6. PERSISTENCE - Reuse an RDD's computed result across multiple actions")
print("=" * 80)

# Without caching, each action below would re-run map() from scratch since
# transformations are lazy and RDDs aren't stored by default.
print("\n--- cache() - Keep squared_rdd's results in memory after the first action ---")
squared_rdd.cache()
print(f"First action (computes and caches): count = {squared_rdd.count()}")
print(f"Second action (reads from cache, doesn't recompute): sum = {squared_rdd.sum()}")
squared_rdd.unpersist()

# ============================================================================
# SECTION 7: RDD <-> DATAFRAME
# ============================================================================
print("\n" + "=" * 80)
print("7. RDD <-> DATAFRAME - Converting between the two APIs")
print("=" * 80)

print("\n--- toDF() - Build a DataFrame from a pair RDD ---")
dept_totals_df = total_by_dept.toDF(["Department", "TotalSalary"])
dept_totals_df.orderBy("Department").show()

print("\n--- df.rdd - Drop back down to the underlying RDD of Row objects ---")
rows = dept_totals_df.rdd.map(lambda row: (row.Department, row.TotalSalary)).collect()
print(f"Back to plain tuples: {sorted(rows)}")

print("\n--- spark.read.csv() + .rdd - Convert a real dataset to an RDD ---")
dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Datasets", "melb_data.csv")
melb_df = spark.read.csv(dataset_path, header=True, inferSchema=True)
melb_rdd = melb_df.rdd  # DataFrame -> RDD of Row objects, one Row per CSV record

print(f"Melbourne housing dataset: {melb_rdd.count()} rows")
# Prefer collect() over take()/first() on an RDD converted straight from a
# DataFrame: on some local Windows setups, take()/first()'s incremental
# partition-scanning strategy crashes the Python worker for this kind of RDD,
# while count()/collect()/reduceByKey() go through a different, more stable
# code path. limit(1) narrows it down on the DataFrame side first, so the
# .rdd conversion and collect() below only ever touch a single row.
print(f"First row as a Row object: {melb_df.limit(1).rdd.collect()[0]}")

print("\nAverage price for the first 5 suburbs (alphabetically), computed with plain RDD ops:")
suburb_price_rdd = melb_rdd.map(lambda row: (row.Suburb, row.Price)) \
    .filter(lambda kv: kv[1] is not None)
suburb_sum_count_rdd = suburb_price_rdd.mapValues(lambda price: (price, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
suburb_avg_rdd = suburb_sum_count_rdd.mapValues(lambda sum_count: round(sum_count[0] / sum_count[1], 2))
for suburb, avg_price in suburb_avg_rdd.sortByKey().collect()[:5]:
    print(f"  {suburb}: ${avg_price:,}")

print("\n" + "=" * 80)
print("END OF PYSPARK RDDs EXAMPLES")
print("=" * 80)

spark.stop()
