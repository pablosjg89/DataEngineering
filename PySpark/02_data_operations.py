"""
Data Operations with PySpark
Examples of filtering, grouping, and aggregating data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder \
    .appName("DataOperations") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create a sample DataFrame
data = [
    ("Alice", "Sales", 50000),
    ("Bob", "Engineering", 80000),
    ("Charlie", "Sales", 60000),
    ("David", "Engineering", 75000),
    ("Eve", "HR", 55000),
]
columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

# Filter operations
print("\nEngineering department employees:")
df.filter(col("Department") == "Engineering").show()

print("\nEmployees with salary > 60000:")
df.filter(col("Salary") > 60000).show()

# Using where (alias of filter) with compound conditions
print("\nEmployees with salary between 50000 and 80000:")
df.where((col("Salary") >= 50000) & (col("Salary") <= 80000)).show()

# Sorting examples (orderBy / sort)
print("\nSort by Salary ascending:")
df.orderBy(col("Salary").asc()).show()

print("\nSort by Salary descending, then Name ascending:")
df.orderBy(col("Salary").desc(), col("Name").asc()).show()

print("\nSort using sort() alias (Salary descending):")
df.sort(col("Salary").desc()).show()

# Simple use case: a straightforward alphabetical sort for reporting
print("\nSimple use case - alphabetical sort by Name:")
df.sort("Name").show()

print("\nTop 2 highest-paid employees:")
df.orderBy(col("Salary").desc()).limit(2).show()

# Handling missing values (na.drop)
print("\nExample DataFrame with nulls:")
data_with_nulls = [
    ("Alice", "Sales", 50000),
    ("Frank", None, None),
    ("Grace", "HR", None),
]
cols = ["Name", "Department", "Salary"]
null_df = spark.createDataFrame(data_with_nulls, cols)
null_df.show()

print("\nDrop rows with ANY nulls (na.drop() default):")
null_df.na.drop().show()

print("\nDrop rows where 'Salary' is null only:")
null_df.na.drop(subset=["Salary"]).show()

print("\nFill null salaries with 0 and show sorted by Salary:")
filled = null_df.na.fill({"Salary": 0})
filled.orderBy(col("Salary").desc()).show()

# --- Alternate null-handling APIs ---
print("\nAlternate null-handling APIs:")
# 1) Using where() with isNotNull()
print("\nRows where Salary is not null (using where(...).isNotNull()):")
df.where(col("Salary").isNotNull()).show()

# 2) na.drop(how='all') - drops rows where ALL columns are null
print("\nDrop rows where ALL columns are null (na.drop(how='all')):")
all_nulls = [ (None, None, None), ("Hank", None, 45000) ]
all_nulls_df = spark.createDataFrame(all_nulls, cols)
all_nulls_df.show()
all_nulls_df.na.drop(how="all").show()

# 3) na.fill() for multiple columns
print("\nFill nulls with defaults (na.fill) for multiple columns:")
filled_multi = null_df.na.fill({"Department": "Unknown", "Salary": -1})
filled_multi.show()

print("\nAfter filling, sort by Salary to see defaulted rows at bottom:")
filled_multi.sort(col("Salary").desc()).show()

# Use case: sorting to create a deterministic leaderboard before export or reporting
# Sorting provides deterministic order for downstream consumers and makes "top N" selection trivial.
print("\nUse case - Leaderboard: Employees ordered by Salary (descending), Name ascending:")
leaderboard = df.orderBy(col("Salary").desc(), col("Name").asc())
leaderboard.show()

# Example: grab top 3 for a report
print("\nTop 3 employees for a report:")
leaderboard.limit(3).show()

# Column operations: derived columns, renaming, and dropping
print("\nColumn operations examples:")
# 1) withColumn - create a derived metric (10% raise)
print("\nAdd derived column 'SalaryAfterRaise' (10% raise):")
with_raise = df.withColumn("SalaryAfterRaise", (col("Salary") * 1.10))
with_raise.select("Name", "Salary", "SalaryAfterRaise").show()

# 2) chain withColumn to create another transformed column
print("\nAdd 'SalaryK' derived from SalaryAfterRaise (in thousands):")
with_raise = with_raise.withColumn("SalaryK", (col("SalaryAfterRaise") / 1000))
with_raise.select("Name", "SalaryAfterRaise", "SalaryK").show()

# 3) withColumnRenamed - improve clarity
print("\nRename 'Salary' to 'BaseSalary' for clarity:")
renamed = df.withColumnRenamed("Salary", "BaseSalary")
renamed.show()

# 4) drop - remove redundant columns to focus dataset
print("\nDrop 'Department' column to focus on salaries:")
dropped = renamed.drop("Department")
dropped.show()

# 5) combined: compute derived, rename, and drop in a pipeline
print("\nPipeline: compute, rename, and drop in one chain:")
pipeline = (
    df.withColumn("SalaryAfterRaise", col("Salary") * 1.10)
      .withColumn("SalaryK", col("SalaryAfterRaise") / 1000)
      .withColumnRenamed("Salary", "BaseSalary")
      .drop("Department")
)
pipeline.show()

# Row-level filters examples
print("\nRow-level filters: salaries between 55000 and 80000 and in Engineering or Sales:")
df.filter((col("Salary") >= 55000) & (col("Salary") <= 80000) & (col("Department").isin(["Engineering", "Sales"]))).show()

print("\nFilter using SQL-style string expression (Department = 'HR'):")
df.filter("Department = 'HR'").show()

print("\nFilter using LIKE (names starting with 'A'):")
df.filter(col("Name").like("A% ") ).show() if False else df.filter(col("Name").like("A%" )).show()

# Group by and aggregate examples
print("\nAverage salary by department:")
df.groupBy("Department").agg(
    avg("Salary").alias("AvgSalary"),
    count("*").alias("Count")
).show()

print("\nAggregations with ordering: average salary by department, descending:")
df.groupBy("Department").agg(avg("Salary").alias("AvgSalary"), count("*").alias("Count")).orderBy(col("AvgSalary").desc()).show()

print("\nGroup by with filter on aggregate (departments with avg salary > 60000):")
from pyspark.sql import functions as F
agg_df = df.groupBy("Department").agg(F.avg("Salary").alias("AvgSalary"), F.count("*").alias("Count"))
agg_df.filter(col("AvgSalary") > 60000).show()

# Example: combined grouping with multiple aggregations and renaming
print("\nDetailed department stats (avg, min, max, count):")
df.groupBy("Department").agg(
    F.avg("Salary").alias("AvgSalary"),
    F.min("Salary").alias("MinSalary"),
    F.max("Salary").alias("MaxSalary"),
    F.count("*").alias("EmployeeCount")
).orderBy(col("AvgSalary").desc()).show()

# Select specific columns
print("\nNames and Departments:")
df.select("Name", "Department").show()

spark.stop()
