"""
Understanding pyspark.sql.functions (F library)

The 'functions' module provides SQL-like functions for DataFrame operations.
Import as: from pyspark.sql import functions as F

Key Function Categories:
- Column expressions: col(), lit(), concat()
- String operations: upper(), lower(), length(), substring(), trim()
- Conditional logic: when(), otherwise(), coalesce()
- Aggregations: avg(), count(), sum(), min(), max(), stddev()
- Date/time: current_date(), date_add(), datediff()
- Window functions: row_number(), rank(), dense_rank(), lag(), lead()

Why use F (functions)?
1. SQL-style operations familiar to SQL users
2. Prevents name conflicts: F.sum() vs Python's sum()
3. Enables expression composition: F.upper(F.col("name"))
4. Type safety: returns Column objects usable in operations
5. Optimization: Spark optimizes F.* calls in lazy evaluation
6. Standard practice: PySpark community convention
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    # Column functions
    col, lit, concat, coalesce,
    # String functions
    upper, lower, length, substring, trim, ltrim, rtrim,
    # Conditional functions
    when, otherwise,
    # Aggregation functions
    avg, sum, count, min, max, stddev,
    # Date/time functions
    current_date, date_add, datediff, year, month, day,
    # Window functions
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

print("=" * 80)
print("PYSPARK SQL FUNCTIONS LIBRARY (F)")
print("=" * 80)

print("\nOriginal DataFrame:")
df.show()

# ============================================================================
# SECTION 1: COLUMN FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("1. COLUMN FUNCTIONS - Reference, create, and manipulate columns")
print("=" * 80)

# col() - creates a Column reference
print("\n--- col() - Reference existing columns ---")
print("Select Name and Salary columns:")
df.select(col("Name"), col("Salary")).show()

# lit() - creates literal constant values
print("\n--- lit() - Create constant/literal values ---")
print("Add constant columns (Status='Active', Year=2024):")
df.select(col("Name"), lit("Active").alias("Status"), lit(2024).alias("Year")).show()

# concat() - concatenate/join strings
print("\n--- concat() - Concatenate multiple strings ---")
print("Combine Name and Department:")
df.select(concat(col("Name"), lit(" - "), col("Department")).alias("NameDept")).show()

# coalesce() - return first non-null value
print("\n--- coalesce() - Return first non-null value ---")
print("Department with 'Unknown' fallback for nulls:")
df.select(
    col("Name"),
    coalesce(col("Department"), lit("Unknown")).alias("DepartmentOrUnknown")
).show()

# ============================================================================
# SECTION 2: STRING FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("2. STRING FUNCTIONS - Manipulate and extract text data")
print("=" * 80)

# upper() and lower() - change case
print("\n--- upper() / lower() - Convert text case ---")
print("Uppercase Department, lowercase Name:")
df.select(col("Name"), upper(col("Department")).alias("DeptUpper"), lower(col("Name")).alias("NameLower")).show()

# length() - string length
print("\n--- length() - Get string length ---")
print("Length of Name column:")
df.select(col("Name"), length(col("Name")).alias("NameLength")).show()

# substring() - extract part of string
print("\n--- substring() - Extract substring ---")
print("First 3 characters of Name:")
df.select(col("Name"), substring(col("Name"), 1, 3).alias("First3Chars")).show()

# trim(), ltrim(), rtrim() - remove whitespace
print("\n--- trim() / ltrim() / rtrim() - Remove whitespace ---")
names_with_space = [("  Alice  ",), ("Bob   ",), ("  Charlie",)]
space_df = spark.createDataFrame(names_with_space, ["Name"])
print("Original (with spaces):")
space_df.show(truncate=False)
print("After trim() (remove all):")
space_df.select(trim(col("Name")).alias("Trimmed")).show(truncate=False)
print("After ltrim() (left trim):")
space_df.select(ltrim(col("Name")).alias("LeftTrimmed")).show(truncate=False)
print("After rtrim() (right trim):")
space_df.select(rtrim(col("Name")).alias("RightTrimmed")).show(truncate=False)

# ============================================================================
# SECTION 3: CONDITIONAL FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("3. CONDITIONAL FUNCTIONS - IF-THEN-ELSE logic")
print("=" * 80)

# when().otherwise() - SQL-like IF-THEN-ELSE
print("\n--- when() / otherwise() - Conditional branching ---")
print("Salary brackets (High/Medium/Low):")
df.select(
    col("Name"),
    col("Salary"),
    when(col("Salary") >= 75000, "High")
    .when(col("Salary") >= 65000, "Medium")
    .otherwise("Low").alias("SalaryBracket")
).show()

# ============================================================================
# SECTION 4: AGGREGATION FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("4. AGGREGATION FUNCTIONS - Compute summary statistics")
print("=" * 80)

# Basic aggregations: avg, sum, count, min, max
print("\n--- Basic aggregations: avg, sum, count, min, max ---")
print("Salary statistics across all employees:")
df.select(
    avg("Salary").alias("AvgSalary"),
    sum("Salary").alias("TotalSalary"),
    count("*").alias("Count"),
    min("Salary").alias("MinSalary"),
    max("Salary").alias("MaxSalary")
).show()

# stddev() - standard deviation
print("\n--- stddev() - Standard deviation ---")
print("Salary standard deviation:")
df.select(stddev("Salary").alias("SalaryStdDev")).show()

# GroupBy with aggregations
print("\n--- GroupBy with aggregations ---")
print("Department statistics:")
df.groupBy("Department").agg(
    count("*").alias("EmployeeCount"),
    avg("Salary").alias("AvgSalary"),
    min("Salary").alias("MinSalary"),
    max("Salary").alias("MaxSalary"),
    stddev("Salary").alias("SalaryStdDev")
).show()

# ============================================================================
# SECTION 5: DATE/TIME FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("5. DATE/TIME FUNCTIONS - Work with dates")
print("=" * 80)

# current_date() - today's date
print("\n--- current_date() - Get today's date ---")
print("Add current date column:")
df.select(col("Name"), col("JoinDate"), current_date().alias("Today")).show()

# date_add() - add days to date
print("\n--- date_add() - Add days to a date ---")
print("Add 365 days to JoinDate (anniversary date):")
df.select(col("Name"), col("JoinDate"), date_add(col("JoinDate"), 365).alias("OneYearAnniversary")).show()

# datediff() - difference in days
print("\n--- datediff() - Calculate days between dates ---")
print("Days since joining:")
df.select(
    col("Name"),
    col("JoinDate"),
    datediff(current_date(), col("JoinDate")).alias("DaysSinceJoin")
).show()

# year(), month(), day() - extract date parts
print("\n--- year() / month() / day() - Extract date components ---")
print("Join year, month, day:")
df.select(
    col("Name"),
    col("JoinDate"),
    year(col("JoinDate")).alias("JoinYear"),
    month(col("JoinDate")).alias("JoinMonth"),
    day(col("JoinDate")).alias("JoinDay")
).show()

# ============================================================================
# SECTION 6: WINDOW FUNCTIONS
# ============================================================================
print("\n" + "=" * 80)
print("6. WINDOW FUNCTIONS - Compute values over row partitions")
print("=" * 80)

# row_number() - sequential numbering within partition
print("\n--- row_number() - Sequential numbering per partition ---")
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())
print("Row number by salary within each department:")
df.select(
    col("Name"),
    col("Department"),
    col("Salary"),
    row_number().over(window_spec).alias("SalaryRankInDept")
).show()

# rank() - rank with ties
print("\n--- rank() - Ranking with tie handling ---")
rank_window = Window.orderBy(col("Salary").desc())
print("Salary rank (ties get same rank):")
df.select(
    col("Name"),
    col("Salary"),
    rank().over(rank_window).alias("SalaryRank")
).show()

# dense_rank() - rank without gaps
print("\n--- dense_rank() - Dense ranking without gaps ---")
print("Dense rank by salary (no gaps after ties):")
df.select(
    col("Name"),
    col("Salary"),
    dense_rank().over(rank_window).alias("DenseSalaryRank")
).show()

# lag() - access previous row value
print("\n--- lag() - Access previous row value ---")
lag_window = Window.orderBy(col("Salary").asc())
print("Previous employee's salary (ordered by salary):")
df.select(
    col("Name"),
    col("Salary"),
    lag(col("Salary"), 1).over(lag_window).alias("PreviousSalary")
).show()

# lead() - access next row value
print("\n--- lead() - Access next row value ---")
print("Next employee's salary (ordered by salary):")
df.select(
    col("Name"),
    col("Salary"),
    lead(col("Salary"), 1).over(lag_window).alias("NextSalary")
).show()

print("\n" + "=" * 80)
print("END OF PYSPARK SQL FUNCTIONS EXAMPLES")
print("=" * 80)

spark.stop()
