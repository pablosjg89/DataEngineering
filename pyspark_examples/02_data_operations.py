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

print("\nTop 2 highest-paid employees:")
df.orderBy(col("Salary").desc()).limit(2).show()

# Group by and aggregate
print("\nAverage salary by department:")
df.groupBy("Department").agg(
    avg("Salary").alias("AvgSalary"),
    count("*").alias("Count")
).show()

# Select specific columns
print("\nNames and Departments:")
df.select("Name", "Department").show()

spark.stop()
