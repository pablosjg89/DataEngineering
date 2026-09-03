"""
Understanding pyspark.sql.functions (F library)
This file demonstrates the F library and why it's essential for PySpark.

The 'functions' module provides SQL-like functions for DataFrame operations.
Import as: from pyspark.sql import functions as F
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, concat, upper, lower, length, substring,
    when, otherwise, coalesce,
    avg, sum, count, min, max, stddev,
    current_date, date_add, datediff,
    row_number, rank, dense_rank, lag, lead
)
from pyspark.sql import Window

spark = SparkSession.builder \
    .appName("FunctionsLibrary") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Sample data
data = [
    ("Alice", "Engineering", 80000, "2020-01-15"),
    ("Bob", "Sales", 65000, "2019-06-20"),
    ("Charlie", "Engineering", 75000, "2021-03-10"),
    ("Diana", "Sales", 70000, "2020-11-05"),
    ("Eve", None, 55000, "2022-02-28"),
]
columns = ["Name", "Department", "Salary", "JoinDate"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# --- Column Functions ---
print("\n--- COLUMN FUNCTIONS ---")

# col() - creates a Column reference
print("\nUsing col() to reference columns:")
df.select(col("Name"), col("Salary")).show()

# lit() - creates literal values
print("\nUsing lit() to add constant values:")
df.select(col("Name"), lit("Active").alias("Status"), lit(2024).alias("Year")).show()

# concat() - concatenate strings
print("\nConcatenate strings with concat():")
df.select(concat(col("Name"), lit(" - "), col("Department")).alias("NameDept")).show()

# --- String Functions ---
print("\n--- STRING FUNCTIONS ---")

# upper(), lower()
print("\nUppercase and lowercase:")
df.select(col("Name"), upper(col("Department")).alias("DeptUpper"), lower(col("Name")).alias("NameLower")).show()

# length() - string length
print("\nString length:")
df.select(col("Name"), length(col("Name")).alias("NameLength")).show()

# substring() - extract part of string
print("\nSubstring (first 3 chars of Name):")
df.select(col("Name"), substring(col("Name"), 1, 3).alias("First3Chars")).show()

# --- Conditional Functions ---
print("\n--- CONDITIONAL FUNCTIONS ---")

# when().otherwise() - SQL-like IF-THEN-ELSE
print("\nSalary brackets with when().otherwise():")
df.select(
    col("Name"),
    col("Salary"),
    when(col("Salary") >= 75000, "High")
    .when(col("Salary") >= 65000, "Medium")
    .otherwise("Low").alias("SalaryBracket")
).show()

# coalesce() - return first non-null value
print("\nCoalesce (Department with 'Unknown' fallback):")
df.select(
    col("Name"),
    coalesce(col("Department"), lit("Unknown")).alias("DepartmentOrUnknown")
).show()

# --- Aggregation Functions ---
print("\n--- AGGREGATION FUNCTIONS ---")

# Basic aggregations
print("\nAggregation functions (avg, sum, count, min, max):")
df.select(
    avg("Salary").alias("AvgSalary"),
    sum("Salary").alias("TotalSalary"),
    count("*").alias("Count"),
    min("Salary").alias("MinSalary"),
    max("Salary").alias("MaxSalary")
).show()

# stddev() - standard deviation
print("\nStandard deviation of Salary:")
df.select(stddev("Salary").alias("SalaryStdDev")).show()

# GroupBy with aggregations
print("\nGroupBy with multiple aggregations:")
df.groupBy("Department").agg(
    count("*").alias("EmployeeCount"),
    avg("Salary").alias("AvgSalary"),
    min("Salary").alias("MinSalary"),
    max("Salary").alias("MaxSalary")
).show()

# --- Date Functions ---
print("\n--- DATE FUNCTIONS ---")

# current_date()
print("\nAdd current date column:")
df.select(col("Name"), col("JoinDate"), current_date().alias("Today")).show()

# date_add() - add days to date
print("\nAdd 365 days to JoinDate:")
df.select(col("Name"), col("JoinDate"), date_add(col("JoinDate"), 365).alias("OneYearLater")).show()

# datediff() - difference in days
print("\nDays since joining (JoinDate to Today):")
df.select(
    col("Name"),
    col("JoinDate"),
    datediff(current_date(), col("JoinDate")).alias("DaysSinceJoin")
).show()

# --- Window Functions ---
print("\n--- WINDOW FUNCTIONS ---")

# row_number() - sequential numbering within partition
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())

print("\nRow number by salary within each department:")
df.select(
    col("Name"),
    col("Department"),
    col("Salary"),
    row_number().over(window_spec).alias("RankByDeptSalary")
).show()

# rank() - rank with ties
print("\nRank by salary (with ties):")
rank_window = Window.orderBy(col("Salary").desc())
df.select(
    col("Name"),
    col("Salary"),
    rank().over(rank_window).alias("SalaryRank")
).show()

# lag() - access previous row
print("\nPrevious employee's salary (when ordered by salary):")
lag_window = Window.orderBy(col("Salary").asc())
df.select(
    col("Name"),
    col("Salary"),
    lag(col("Salary"), 1).over(lag_window).alias("PreviousSalary")
).show()

# --- Why F (functions) is Essential ---
print("\n--- WHY USE F (FUNCTIONS) ---")
print("""
1. SQL-style operations: F.avg(), F.sum() feel familiar to SQL users
2. Prevents name conflicts: F.sum() vs Python's sum()
3. Enables expression composition: chain operations like F.upper(F.col("name"))
4. Type safety: returns Column objects usable in DataFrame operations
5. Optimization: Spark can optimize F.* calls in lazy evaluation
6. Standard practice: PySpark community standard, improves code readability
""")

spark.stop()
