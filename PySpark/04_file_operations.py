"""
File Operations with PySpark
Examples of reading and writing data in various formats (CSV, JSON, Parquet).
"""

from pyspark.sql import SparkSession
import tempfile
import os

spark = SparkSession.builder \
    .appName("FileOperations") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create sample data
data = [
    ("Alice", 25, "New York"),
    ("Bob", 30, "Los Angeles"),
    ("Charlie", 35, "Chicago"),
]
columns = ["Name", "Age", "City"]
df = spark.createDataFrame(data, columns)

# Use a temporary directory for demo
temp_dir = tempfile.gettempdir()

# Write and read CSV
csv_path = os.path.join(temp_dir, "output.csv")
print(f"Writing CSV to {csv_path}")
df.coalesce(1).write.csv(csv_path, header=True, mode="overwrite")

print("Reading CSV:")
csv_df = spark.read.csv(csv_path, header=True, inferSchema=True)
csv_df.show()

# Write and read Parquet
parquet_path = os.path.join(temp_dir, "output.parquet")
print(f"\nWriting Parquet to {parquet_path}")
df.write.parquet(parquet_path, mode="overwrite")

print("Reading Parquet:")
parquet_df = spark.read.parquet(parquet_path)
parquet_df.show()

# Write and read JSON
json_path = os.path.join(temp_dir, "output.json")
print(f"\nWriting JSON to {json_path}")
df.write.json(json_path, mode="overwrite")

print("Reading JSON:")
json_df = spark.read.json(json_path)
json_df.show()

spark.stop()
