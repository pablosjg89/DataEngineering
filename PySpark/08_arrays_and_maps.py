"""
PySpark Arrays and Maps (Complex Data Types)

Arrays and Maps are nested/complex data types in PySpark that enable:
- Array: Ordered collection of elements (similar to Python lists)
- Map: Key-value pairs (similar to Python dictionaries)

Why use Arrays and Maps?
1. Store multiple related values in a single column
2. Handle semi-structured data (JSON, logs)
3. Avoid flattening normalized data structures
4. Enable efficient aggregations on grouped data

Common use cases:
- Store multiple phone numbers or emails per person (Array)
- Store attributes like color, size, price (Map)
- Handle nested JSON structures from APIs
- Process log data with variable-length fields
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, array, map_from_arrays, map_concat, array_contains,
    size, array_join, array_distinct, array_union,
    explode, map_keys, map_values,
    lit, struct
)

spark = SparkSession.builder \
    .appName("ArraysAndMaps") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("PYSPARK ARRAYS AND MAPS - COMPLEX DATA TYPES")
print("=" * 80)

# ============================================================================
# SECTION 1: ARRAYS - ORDERED COLLECTIONS
# ============================================================================
print("\n" + "=" * 80)
print("1. ARRAYS - Ordered collections of elements")
print("=" * 80)

# Create DataFrame with array data
people_data = [
    ("Alice", ["Python", "Java", "Scala"], 5),
    ("Bob", ["Python", "Go"], 3),
    ("Charlie", ["Java", "Kotlin", "Scala", "Go"], 7),
    ("Diana", ["Python"], 2),
]
people_columns = ["Name", "Skills", "YearsExperience"]
df = spark.createDataFrame(people_data, people_columns)

print("\n--- Original DataFrame with Arrays ---")
df.printSchema()
df.show(truncate=False)

# array() - create array literals
print("\n--- array() - Create array literals ---")
df.select(
    col("Name"),
    array(lit("Certified"), lit("Expert")).alias("Certifications")
).show()

# array_contains() - check if element exists in array
print("\n--- array_contains() - Check if element exists in array ---")
print("People who know Python:")
df.select(col("Name"), col("Skills")).filter(
    array_contains(col("Skills"), "Python")
).show()

# size() - get array length
print("\n--- size() - Get array length ---")
print("Number of skills per person:")
df.select(
    col("Name"),
    col("Skills"),
    size(col("Skills")).alias("NumSkills")
).show()

# array_join() - concatenate array elements into string
print("\n--- array_join() - Concatenate array into string ---")
print("Skills as comma-separated string:")
df.select(
    col("Name"),
    array_join(col("Skills"), ", ").alias("SkillsString")
).show(truncate=False)

# array_distinct() - remove duplicates from array
print("\n--- array_distinct() - Remove duplicate elements ---")
skills_with_dupes = [
    ("Alice", ["Python", "Java", "Python", "Scala", "Java"]),
    ("Bob", ["Go", "Python", "Go"]),
]
dupes_df = spark.createDataFrame(skills_with_dupes, ["Name", "SkillsWithDupes"])
print("Original (with duplicates):")
dupes_df.show()
print("After array_distinct():")
dupes_df.select(
    col("Name"),
    array_distinct(col("SkillsWithDupes")).alias("UniqueSkills")
).show()

# array_union() - combine two arrays
print("\n--- array_union() - Combine two arrays ---")
combined_data = [
    ("Alice", ["Python", "Java"], ["Go", "Rust"]),
    ("Bob", ["JavaScript"], ["TypeScript", "Node.js"]),
]
combined_df = spark.createDataFrame(combined_data, ["Name", "BackendSkills", "FrontendSkills"])
print("Merge backend and frontend skills:")
combined_df.select(
    col("Name"),
    col("BackendSkills"),
    col("FrontendSkills"),
    array_union(col("BackendSkills"), col("FrontendSkills")).alias("AllSkills")
).show()

# explode() - flatten array into rows
print("\n--- explode() - Convert array elements into rows ---")
print("Explode skills array (one row per skill):")
df.select(col("Name"), explode(col("Skills")).alias("Skill")).show()

# ============================================================================
# SECTION 2: MAPS - KEY-VALUE PAIRS
# ============================================================================
print("\n" + "=" * 80)
print("2. MAPS - Key-value pair collections")
print("=" * 80)

# Create DataFrame with map data
employees_data = [
    ("Alice", {"email": "alice@company.com", "phone": "555-1234", "office": "NYC"}),
    ("Bob", {"email": "bob@company.com", "phone": "555-5678", "office": "SF"}),
    ("Charlie", {"email": "charlie@company.com", "phone": "555-9999", "office": "NYC"}),
]
employees_columns = ["Name", "ContactInfo"]
emp_df = spark.createDataFrame(employees_data, employees_columns)

print("\n--- Original DataFrame with Maps ---")
emp_df.printSchema()
emp_df.show(truncate=False)

# map_from_arrays() - create map from keys and values arrays
print("\n--- map_from_arrays() - Create maps from parallel arrays ---")
attributes_data = [
    ("Product_A", ["color", "size", "weight"], ["red", "large", "5kg"]),
    ("Product_B", ["color", "size", "weight"], ["blue", "small", "2kg"]),
]
attributes_df = spark.createDataFrame(attributes_data, ["Product", "AttributeKeys", "AttributeValues"])
print("Create product attributes map:")
attributes_df.select(
    col("Product"),
    map_from_arrays(col("AttributeKeys"), col("AttributeValues")).alias("Attributes")
).show(truncate=False)

# map_keys() - get all keys from map
print("\n--- map_keys() - Extract all keys from map ---")
print("Contact info keys (what fields are stored):")
emp_df.select(
    col("Name"),
    map_keys(col("ContactInfo")).alias("InfoFields")
).show()

# map_values() - get all values from map
print("\n--- map_values() - Extract all values from map ---")
print("Contact info values:")
emp_df.select(
    col("Name"),
    map_values(col("ContactInfo")).alias("InfoValues")
).show()

# Accessing map values with bracket notation
print("\n--- Access specific map values with bracket notation ---")
print("Email and phone from contact info:")
emp_df.select(
    col("Name"),
    col("ContactInfo")["email"].alias("Email"),
    col("ContactInfo")["phone"].alias("Phone")
).show()

# map_concat() - merge multiple maps
print("\n--- map_concat() - Merge multiple maps ---")
map_data = [
    ("User1", {"name": "Alice", "role": "Engineer"}, {"department": "Backend", "level": "Senior"}),
    ("User2", {"name": "Bob", "role": "Manager"}, {"department": "Frontend", "level": "Lead"}),
]
map_df = spark.createDataFrame(map_data, ["UserID", "PersonalInfo", "JobInfo"])
print("Merge personal and job information:")
map_df.select(
    col("UserID"),
    map_concat(col("PersonalInfo"), col("JobInfo")).alias("AllInfo")
).show(truncate=False)

# explode() with maps - convert to key-value rows
print("\n--- explode() with maps - Convert to key-value rows ---")
print("Explode contact info (one row per key-value pair):")
emp_df.select(
    col("Name"),
    explode(col("ContactInfo")).alias("InfoType", "InfoValue")
).show()

# ============================================================================
# SECTION 3: ARRAY OF STRUCTS - COMPLEX NESTED DATA
# ============================================================================
print("\n" + "=" * 80)
print("3. ARRAY OF STRUCTS - Complex nested structures")
print("=" * 80)

# Create DataFrame with array of structs
orders_data = [
    ("Order_1", [
        {"product": "Laptop", "quantity": 1, "price": 1200.00},
        {"product": "Mouse", "quantity": 2, "price": 25.00}
    ]),
    ("Order_2", [
        {"product": "Keyboard", "quantity": 1, "price": 80.00},
        {"product": "Monitor", "quantity": 1, "price": 350.00}
    ]),
]
orders_columns = ["OrderID", "Items"]
orders_df = spark.createDataFrame(orders_data, orders_columns)

print("\n--- Original Orders with Array of Structs ---")
orders_df.printSchema()
orders_df.show(truncate=False)

# Explode array of structs
print("\n--- Explode array of structs - Flatten nested data ---")
print("One row per item:")
orders_df.select(
    col("OrderID"),
    explode(col("Items")).alias("Item")
).select(
    col("OrderID"),
    col("Item.product"),
    col("Item.quantity"),
    col("Item.price")
).show()

# ============================================================================
# SECTION 4: PRACTICAL USE CASES
# ============================================================================
print("\n" + "=" * 80)
print("4. PRACTICAL USE CASES")
print("=" * 80)

print("""
Use Case 1: E-Commerce Product Attributes
- Store product features as a map: {"color": "red", "size": "L", "brand": "Nike"}
- Enables flexible schema for different product types
- Easy filtering: WHERE attributes["color"] = "red"

Use Case 2: User Activity Logs
- Store events as array: ["login", "purchase", "logout"]
- Analyze user journeys: array_contains(events, "purchase")
- Track sequence of actions

Use Case 3: Multi-value Attributes
- Store multiple phone numbers as array: ["555-1234", "555-5678"]
- Store multilingual descriptions as map: {"en": "Table", "es": "Mesa"}
- Handle one-to-many relationships efficiently

Use Case 4: Semi-structured Data
- Parse JSON API responses with nested arrays/objects
- Store original structure without normalization
- Extract relevant fields as needed using explode()

Best Practices:
1. Use arrays for homogeneous collections (same type)
2. Use maps for heterogeneous key-value data
3. Use explode() to flatten for SQL-style analysis
4. Validate array/map structure before aggregations
5. Consider normalization vs. keeping nested data
""")

spark.stop()
