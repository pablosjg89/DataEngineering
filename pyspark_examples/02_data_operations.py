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
