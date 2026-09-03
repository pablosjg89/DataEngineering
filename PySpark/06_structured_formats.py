"""
Structured Formats: Reading nested JSON and partitioned Parquet

Examples:
- Read multi-line nested JSON and extract nested fields
- Read partitioned Parquet with schema merging and filter pushdown
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import tempfile
import os
import json

spark = SparkSession.builder \
    .appName("StructuredFormats") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Example 1: Nested JSON (multi-line) ---
nested = [
    {
        "id": 1,
        "user": {"name": "Alice", "email": "alice@example.com"},
        "events": [
            {"type": "click", "ts": "2023-01-01T12:00:00Z"},
            {"type": "purchase", "ts": "2023-01-01T12:05:00Z", "amount": 29.99}
        ]
    },
    {
        "id": 2,
        "user": {"name": "Bob", "email": "bob@example.com"},
        "events": [
            {"type": "view", "ts": "2023-01-02T09:00:00Z"}
        ]
    }
]

# Write nested JSON to a temp file as multi-line JSON (one object per file for demo)
tmp_dir = tempfile.mkdtemp()
json_path = os.path.join(tmp_dir, "nested.json")
with open(json_path, "w", encoding="utf-8") as f:
    for obj in nested:
        f.write(json.dumps(obj) + "\n")

print("Wrote nested JSON to:", json_path)

# Read multi-line JSON (one JSON object per line)
json_df = spark.read.option("multiLine", False).json(json_path)
print("Nested JSON schema:")
json_df.printSchema()
print("Flattened user fields and exploded events:")
json_df.select(
    col("id"),
    col("user.name").alias("user_name"),
    col("user.email").alias("user_email"),
    explode(col("events")).alias("event")
).select("id", "user_name", "user_email", col("event.type"), col("event.ts"), col("event.amount")).show(truncate=False)

# --- Example 2: Partitioned Parquet + schema merging ---
# Create sample DataFrames partitioned by country
people1 = [(1, "Alice", 30, "US"), (2, "Bob", 25, "US")]
people2 = [(3, "Carlos", 28, "ES"), (4, "Diana", 32, "ES")]
cols = ["id", "name", "age", "country"]

df1 = spark.createDataFrame(people1, cols)
df2 = spark.createDataFrame(people2, cols)

parquet_dir = os.path.join(tmp_dir, "people_parquet")
# write partitioned by country
(df1.write.mode("overwrite").partitionBy("country").parquet(parquet_dir))
(df2.write.mode("append").partitionBy("country").parquet(parquet_dir))

print("Wrote partitioned Parquet to:", parquet_dir)

# Read partitioned parquet and let Spark discover partitions
# Enable schema merging if different files had different schemas (demo uses same schema)
parquet_df = spark.read.option("mergeSchema", True).parquet(parquet_dir)
print("Partitioned Parquet schema:")
parquet_df.printSchema()
print("Data from partitioned Parquet:")
parquet_df.show()

# Filter pushdown example (predicate on partition column)
print("Rows for country=ES (partition filter pushdown):")
parquet_df.filter(col("country") == "ES").show()

spark.stop()
