"""
Basic PySpark Setup and Initialization
This example demonstrates how to create a SparkSession and perform basic operations.
"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BasicSparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Create a simple DataFrame from a list
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Display the DataFrame
print("DataFrame created:")
df.show()

# Get schema information
print("DataFrame Schema:")
df.printSchema()

# Stop the session
spark.stop()
