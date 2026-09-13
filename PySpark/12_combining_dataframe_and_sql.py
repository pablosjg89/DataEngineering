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
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

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

print("\n" + "=" * 80)
print("END OF COMBINING DATAFRAME AND SQL OPERATIONS EXAMPLES")
print("=" * 80)

spark.stop()
