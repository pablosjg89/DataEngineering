"""
Joins and Unions with PySpark
Examples of combining multiple DataFrames.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("JoinsAndUnions") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create first DataFrame (Employees)
employees = [
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 101),
    (4, "David", 103),
]
emp_columns = ["EmployeeID", "Name", "DepartmentID"]
emp_df = spark.createDataFrame(employees, emp_columns)

# Create second DataFrame (Departments)
departments = [
    (101, "Sales"),
    (102, "Engineering"),
]
dept_columns = ["DepartmentID", "DepartmentName"]
dept_df = spark.createDataFrame(departments, dept_columns)

print("Employees:")
emp_df.show()

print("\nDepartments:")
dept_df.show()

# --- Basic Join Examples (same column names) ---
print("\n--- BASIC JOINS (common column name) ---")

# Inner Join (default) - explicit on and how parameters
print("\nInner Join (only matching records):")
inner_join_df = emp_df.join(dept_df, on="DepartmentID", how="inner")
inner_join_df.show()

# Left Join (all from left, matching from right)
print("\nLeft Join (all employees, matching departments):")
left_join_df = emp_df.join(dept_df, on="DepartmentID", how="left")
left_join_df.show()

# Right Join (all from right, matching from left)
print("\nRight Join (all departments, matching employees):")
right_join_df = emp_df.join(dept_df, on="DepartmentID", how="right")
right_join_df.show()

# Outer Join (all records from both sides)
print("\nOuter Join (all records from both sides):")
outer_join_df = emp_df.join(dept_df, on="DepartmentID", how="outer")
outer_join_df.show()

# --- Join with Different Column Names ---
print("\n--- JOINS WITH DIFFERENT COLUMN NAMES ---")

# Create DataFrames with different column names for the join key
salaries = [
    (1, 50000),
    (2, 80000),
    (3, 60000),
    (5, 70000),
]
sal_columns = ["PersonID", "Salary"]
sal_df = spark.createDataFrame(salaries, sal_columns)

print("\nSalaries DataFrame:")
sal_df.show()

# Join with different column names using explicit condition
print("\nJoin employees and salaries (EmployeeID = PersonID) - Inner:")
salary_join = emp_df.join(sal_df, on=(col("EmployeeID") == col("PersonID")), how="inner")
salary_join.show()

print("\nLeft Join: all employees, matching salaries:")
salary_left_join = emp_df.join(sal_df, on=(col("EmployeeID") == col("PersonID")), how="left")
salary_left_join.show()

# --- Multiple Field Joins ---
print("\n--- JOINS ON MULTIPLE FIELDS ---")

# Create DataFrames with composite keys
projects = [
    (1, 101, "ProjectA"),
    (2, 102, "ProjectB"),
    (3, 101, "ProjectC"),
]
proj_columns = ["EmployeeID", "DepartmentID", "Project"]
proj_df = spark.createDataFrame(projects, proj_columns)

assignments = [
    (1, 101, "Active"),
    (2, 102, "Active"),
    (3, 101, "Inactive"),
    (4, 101, "Pending"),
]
assign_columns = ["EmployeeID", "DepartmentID", "Status"]
assign_df = spark.createDataFrame(assignments, assign_columns)

print("\nProjects DataFrame:")
proj_df.show()

print("\nAssignments DataFrame:")
assign_df.show()

# Join on multiple fields
print("\nInner Join on multiple fields (EmployeeID AND DepartmentID):")
multi_join = proj_df.join(
    assign_df,
    on=((col("proj_df.EmployeeID") == col("assign_df.EmployeeID")) & 
        (col("proj_df.DepartmentID") == col("assign_df.DepartmentID"))),
    how="inner"
)
multi_join.show()

# Alternative: using list of column names (when names match)
print("\nLeft Join on multiple fields (using implicit names):")
multi_left_join = proj_df.join(
    assign_df,
    on=["EmployeeID", "DepartmentID"],
    how="left"
)
multi_left_join.show()

# --- Full Outer Join with Multiple Conditions ---
print("\nFull Outer Join on multiple fields:")
multi_outer_join = proj_df.join(
    assign_df,
    on=["EmployeeID", "DepartmentID"],
    how="outer"
)
multi_outer_join.show()

# Create another DataFrame for union
more_employees = [
    (5, "Frank", 104),
]
more_emp_df = spark.createDataFrame(more_employees, emp_columns)

# Union
print("\n--- UNION ---")
print("""
UNION combines rows from two or more DataFrames INTO A SINGLE DataFrame.
CRITICAL: Both DataFrames MUST have the SAME SCHEMA (same column names and types).

Why schema must match:
- Column names: Union expects columns in the same order
- Data types: Each column must have matching types (Int, String, etc.)
- Column count: Both DataFrames must have the same number of columns

If schemas don't match: Use unionByName() instead, or select/rename columns first.
""")

print("\n--- Schema Validation ---")
print("Employee DataFrame schema:")
emp_df.printSchema()
print("\nMore Employees DataFrame schema:")
more_emp_df.printSchema()
print("\n✓ Schemas match! Both have: (EmployeeID: int, Name: string, DepartmentID: int)")

print("\n--- Simple Union (same schema) ---")
print("\nUnion of two employee DataFrames (combining rows):")
combined_df = emp_df.union(more_emp_df)
combined_df.show()

# Example showing what happens with different schemas
print("\n--- Schema Mismatch Example (unionByName alternative) ---")
diff_schema_data = [
    ("Alice", 101, 80000),  # Different column order!
    ("Bob", 102, 65000),
]
diff_schema_columns = ["Name", "DepartmentID", "Salary"]
diff_df = spark.createDataFrame(diff_schema_data, diff_schema_columns)

print("DataFrame with different schema (Name, DepartmentID, Salary):")
diff_df.printSchema()

print("\nAttempting union with different schema would fail or produce incorrect results.")
print("Solution: Use unionByName() to match columns by name instead of position:")
# This would work: combined_with_names = emp_df.select("EmployeeID", "Name", "DepartmentID").unionByName(diff_df.select("EmployeeID", "Name", "DepartmentID"))

print("\n--- Union Use Cases ---")
print("""
Common scenarios for Union:
1. Combining historical data with new records (same period format)
2. Merging results from different queries (same fields)
3. Aggregating data from multiple sources (standardized schemas)
4. Appending batch data from multiple files with identical structure
""")

spark.stop()
