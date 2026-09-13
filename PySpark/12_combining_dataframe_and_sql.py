"""
Combining DataFrame and SQL Operations

DataFrame API calls and SQL queries both compile down to the same Catalyst
logical plan - a spark.sql() call returns an ordinary DataFrame, and any
DataFrame can be queried with SQL once it's registered as a temp view. They
aren't two separate systems you have to pick between per script; they're two
syntaxes for building the same execution plan, freely mixable line to line.

This file focuses on that mixing, building on:
- 05_sql_queries.py - spark.sql() basics
- 11_spark_sql_advanced.py - views, catalog, joins, set ops, query plans

Covers:
- Proof the two APIs are interchangeable: an equivalent DataFrame-API query
  and SQL query produce the identical physical plan
- DataFrame -> SQL: build/filter with the DataFrame API, then query it with SQL
- SQL -> DataFrame: take a spark.sql() result and keep transforming it with
  select()/filter()/withColumn()/groupBy()
- expr() - embed a SQL expression inside a DataFrame API call
- selectExpr() - a select() shorthand using SQL-like expression strings
- A multi-step pipeline on a real dataset that hops between both APIs
- Why validating/standardizing types before aggregating matters: a numeric
  column that arrives as a string (a common CSV/JSON ingestion problem) can
  break sum()/avg() outright, or - handled carelessly - silently corrupt
  results. cast() and try_cast() are how you standardize and validate first.
- RDDs for aggregations: the same SUM/AVG done with a custom lambda via
  rdd.map() + reduceByKey(), contrasted with the one-line DataFrame/SQL
  GROUP BY that does the same thing
- Best practices for PySpark aggregations, tying the above together
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as spark_sum, avg, count

spark = SparkSession.builder \
    .appName("CombiningDataFrameAndSQL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

employees = [
    ("Alice", "Engineering", "Senior", 95000),
    ("Bob", "Sales", "Junior", 55000),
    ("Charlie", "Engineering", "Junior", 70000),
    ("Diana", "Sales", "Senior", 85000),
    ("Eve", "HR", "Senior", 60000),
    ("Frank", "Engineering", "Senior", 100000),
]
employees_df = spark.createDataFrame(employees, ["Name", "Department", "Level", "Salary"])
employees_df.createOrReplaceTempView("employees")

print("=" * 80)
print("COMBINING DATAFRAME AND SQL OPERATIONS")
print("=" * 80)

# ============================================================================
# SECTION 1: SAME ENGINE, SAME PLAN - proof the two APIs are interchangeable
# ============================================================================
print("\n" + "=" * 80)
print("1. SAME ENGINE, SAME PLAN - DataFrame API and SQL compile identically")
print("=" * 80)

dataframe_api_query = employees_df.filter(col("Salary") > 60000) \
    .groupBy("Department") \
    .avg("Salary")

sql_query = spark.sql("""
    SELECT Department, AVG(Salary) AS `avg(Salary)`
    FROM employees
    WHERE Salary > 60000
    GROUP BY Department
""")

print("\n--- DataFrame API version ---")
dataframe_api_query.explain()

print("\n--- SQL version ---")
sql_query.explain()

print("Both plans above show the same Filter -> HashAggregate shape: the SQL")
print("parser and the DataFrame API both just build a Catalyst logical plan.")

# ============================================================================
# SECTION 2: DATAFRAME -> SQL - Build/filter with the API, query with SQL
# ============================================================================
print("\n" + "=" * 80)
print("2. DATAFRAME -> SQL - Register a DataFrame API result as a view, then use SQL")
print("=" * 80)

senior_employees_df = employees_df.filter(col("Level") == "Senior")
senior_employees_df.createOrReplaceTempView("senior_employees")

print("\n--- senior_employees_df was built with filter() (DataFrame API) ---")
print("--- but this query runs against it with plain SQL ---")
spark.sql("""
    SELECT Department, COUNT(*) AS SeniorCount, AVG(Salary) AS AvgSeniorSalary
    FROM senior_employees
    GROUP BY Department
    ORDER BY AvgSeniorSalary DESC
""").show()

# ============================================================================
# SECTION 3: SQL -> DATAFRAME - Keep transforming a spark.sql() result
# ============================================================================
print("\n" + "=" * 80)
print("3. SQL -> DATAFRAME - A spark.sql() result is just a DataFrame")
print("=" * 80)

dept_totals_df = spark.sql("""
    SELECT Department, SUM(Salary) AS TotalSalary, COUNT(*) AS HeadCount
    FROM employees
    GROUP BY Department
""")

print("\n--- Result of spark.sql(), continued with select()/withColumn()/orderBy() ---")
dept_totals_df \
    .withColumn("AvgSalary", col("TotalSalary") / col("HeadCount")) \
    .select("Department", "HeadCount", "AvgSalary") \
    .orderBy(col("AvgSalary").desc()) \
    .show()

# ============================================================================
# SECTION 4: expr() - A SQL expression string inside a DataFrame API call
# ============================================================================
print("\n" + "=" * 80)
print("4. expr() - Write a SQL expression as a Column inside the DataFrame API")
print("=" * 80)

print("\n--- filter() with expr() instead of Python comparison operators ---")
employees_df.filter(expr("Salary > 60000 AND Level = 'Senior'")).show()

print("\n--- withColumn() with expr() for a SQL CASE WHEN ---")
employees_df.withColumn(
    "SalaryBand",
    expr("CASE WHEN Salary >= 90000 THEN 'High' WHEN Salary >= 65000 THEN 'Medium' ELSE 'Low' END")
).show()

# ============================================================================
# SECTION 5: selectExpr() - select() with SQL-like expression strings
# ============================================================================
print("\n" + "=" * 80)
print("5. selectExpr() - Shorthand for select(expr(...), expr(...), ...)")
print("=" * 80)

print("\n--- selectExpr() computing and aliasing columns with SQL syntax ---")
employees_df.selectExpr(
    "Name",
    "Department",
    "Salary",
    "ROUND(Salary * 0.10, 2) AS Bonus",
    "Salary > 80000 AS IsTopEarner"
).show()

# ============================================================================
# SECTION 6: FULL PIPELINE - Hopping between both APIs on a real dataset
# ============================================================================
print("\n" + "=" * 80)
print("6. FULL PIPELINE - A real dataset, hopping between DataFrame API and SQL")
print("=" * 80)

dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Datasets", "melb_data.csv")

# Step 1 (DataFrame API): load and do a first-pass filter/cleanup
melb_df = spark.read.csv(dataset_path, header=True, inferSchema=True)
clean_melb_df = melb_df.filter(col("Price").isNotNull() & col("Rooms").isNotNull())
clean_melb_df.createOrReplaceTempView("clean_housing")
print(f"\nStep 1 (DataFrame API): filtered to {clean_melb_df.count()} rows with a Price and Rooms")

# Step 2 (SQL): aggregate per suburb - easier to read as SQL than chained groupBy/agg
print("\nStep 2 (SQL): average price per room, by suburb, for suburbs with 15+ listings")
suburb_stats_df = spark.sql("""
    SELECT
        Suburb,
        COUNT(*) AS Listings,
        ROUND(AVG(Price), 2) AS AvgPrice,
        ROUND(AVG(Price / Rooms), 2) AS AvgPricePerRoom
    FROM clean_housing
    GROUP BY Suburb
    HAVING COUNT(*) >= 15
""")

# Step 3 (DataFrame API): rank the SQL result and pick the top 5 - simpler to
# express as a DataFrame API chain than to fold into the SQL above
print("Step 3 (DataFrame API): sort the SQL result and take the top 5 by AvgPricePerRoom")
suburb_stats_df.orderBy(col("AvgPricePerRoom").desc()).limit(5).show()

# ============================================================================
# SECTION 7: TYPE SAFETY BEFORE AGGREGATING - cast() and try_cast()
# ============================================================================
print("\n" + "=" * 80)
print("7. TYPE SAFETY BEFORE AGGREGATING - cast() and try_cast()")
print("=" * 80)

print("""
Numeric data that arrives as a string (a common CSV/JSON ingestion problem)
can break an aggregation outright, or silently corrupt it if handled
carelessly. Always validate and standardize a column's type before
aggregating it - this protects the pipeline from costly errors downstream.
""")

salary_strings_df = spark.createDataFrame([
    ("Alice", "Engineering", "95000"),
    ("Bob", "Sales", "55000"),
    ("Charlie", "Engineering", "70,000"),  # thousands separator - not a valid number
    ("Diana", "Sales", "85000"),
    ("Eve", "HR", "N/A"),                   # not numeric at all
], ["Name", "Department", "Salary"])  # Salary is inferred as StringType, not numeric

print("--- Salary is a string column - the schema shows no numeric type ---")
salary_strings_df.printSchema()

print("--- sum() straight on the string column blows up (this Spark build runs in ANSI mode) ---")
try:
    salary_strings_df.groupBy("Department").agg(spark_sum("Salary").alias("TotalSalary")).show()
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {str(e).splitlines()[0]}")

print("\n--- cast() to a numeric type also raises on the first malformed value it hits ---")
try:
    salary_strings_df.withColumn("SalaryNum", col("Salary").cast("double")).show()
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {str(e).splitlines()[0]}")

print("\n--- try_cast() converts what it can and returns NULL for the rest, instead of failing ---")
validated_df = salary_strings_df.withColumn("SalaryNum", expr("try_cast(Salary AS DOUBLE)"))
validated_df.show()

print("--- Inspect exactly which rows failed to convert, before they can corrupt an aggregate ---")
validated_df.filter(col("SalaryNum").isNull()).show()

print("--- Now the aggregation runs cleanly on the validated, standardized rows ---")
validated_df.filter(col("SalaryNum").isNotNull()) \
    .groupBy("Department") \
    .agg(spark_sum("SalaryNum").alias("TotalSalary")) \
    .show()

# ============================================================================
# SECTION 8: RDDs FOR AGGREGATIONS - Comparing verbosity with DataFrame/SQL
# ============================================================================
print("\n" + "=" * 80)
print("8. RDDs FOR AGGREGATIONS - The same SUM/AVG, RDD vs. DataFrame/SQL")
print("=" * 80)

print("""
DataFrames/SQL are the preferred choice for most PySpark analytics - SUM()
and AVG() are one line via spark.sql() or the DataFrame API. RDDs have no
built-in aggregation functions like that: getting the same result takes a
custom lambda applied with rdd.map(), then reduceByKey() to combine the
per-row results by key - several lines and a hand-written function instead
of a single GROUP BY.
""")

print("--- DataFrame/SQL: total and average salary per department, one line ---")
spark.sql("""
    SELECT Department, SUM(Salary) AS TotalSalary, AVG(Salary) AS AvgSalary
    FROM employees
    GROUP BY Department
    ORDER BY Department
""").show()

print("--- RDD: the same result, via a custom lambda with map() + reduceByKey() ---")
employees_rdd = employees_df.rdd

# map() turns each Row into (Department, (Salary, 1)) - the "1" is a running
# count so reduceByKey() can total both the salary and the number of rows
dept_salary_pairs_rdd = employees_rdd.map(lambda row: (row.Department, (row.Salary, 1)))

# reduceByKey() applies that lambda across the whole RDD, combining pairs
# that share a key: sum the salaries, sum the counts
dept_totals_rdd = dept_salary_pairs_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# A second lambda turns (TotalSalary, Count) into (TotalSalary, AvgSalary)
dept_agg_rdd = dept_totals_rdd.mapValues(
    lambda total_count: (total_count[0], round(total_count[0] / total_count[1], 2))
)

for department, (total_salary, avg_salary) in sorted(dept_agg_rdd.collect()):
    print(f"  {department}: TotalSalary={total_salary}, AvgSalary={avg_salary}")

print("\nSame result either way - but the RDD version took two custom lambdas and")
print("three chained transformations to express what SQL said in one GROUP BY.")

# ============================================================================
# SECTION 9: BEST PRACTICES FOR PYSPARK AGGREGATIONS
# ============================================================================
print("\n" + "=" * 80)
print("9. BEST PRACTICES FOR PYSPARK AGGREGATIONS")
print("=" * 80)

print("""
1. Filter early - reduce the data size before aggregating, not after. Section
   6's pipeline filtered melb_data.csv down to rows with a Price and Rooms
   BEFORE grouping by Suburb, so the aggregation's shuffle only ever moved
   the rows that actually mattered.

2. Ensure data is clean and correctly typed - see Section 7: a numeric column
   stored as a string can break sum()/avg() outright, or silently corrupt
   the result. Validate and standardize types (cast()/try_cast()) first.

3. Minimize the number of separate aggregation passes - every .groupBy() that
   feeds an action is its own shuffle. Combine multiple statistics into one
   .agg() call instead of running .groupBy() again per statistic (below).

4. Choose the right API - prefer the DataFrame API/SQL over RDDs for
   aggregations in most cases (Section 8): DataFrames get the
   Catalyst/Tungsten optimizer that RDDs don't, with far less code.

5. Monitor performance with .explain() - inspect the physical plan (as
   Section 6 did) to check for avoidable shuffles (Exchange) or missing
   filter pushdown, and adjust the query accordingly.
""")

print("--- Practice #3 in action: one combined .agg() vs. three separate groupBy() calls ---")

print("Three separate groupBy() calls - each .show() triggers its own job and its own shuffle:")
employees_df.groupBy("Department").sum("Salary").show()
employees_df.groupBy("Department").avg("Salary").show()
employees_df.groupBy("Department").count().show()

print("One combined .agg() call - a single groupBy(), a single shuffle, for all three statistics:")
combined_stats_df = employees_df.groupBy("Department").agg(
    spark_sum("Salary").alias("TotalSalary"),
    avg("Salary").alias("AvgSalary"),
    count("*").alias("HeadCount")
)
combined_stats_df.show()

print("Its plan has exactly one Exchange (shuffle) for all three statistics combined:")
combined_stats_df.explain()

print("\n" + "=" * 80)
print("END OF COMBINING DATAFRAME AND SQL OPERATIONS EXAMPLES")
print("=" * 80)

spark.stop()
