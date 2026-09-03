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

# Inner Join (default)
print("\nInner Join (only matching records):")
inner_join_df = emp_df.join(dept_df, "DepartmentID")
inner_join_df.show()

# Left Join (all from left, matching from right)
print("\nLeft Join (all employees, matching departments):")
left_join_df = emp_df.join(dept_df, "DepartmentID", "left")
left_join_df.show()

# Right Join (all from right, matching from left)
print("\nRight Join (all departments, matching employees):")
right_join_df = emp_df.join(dept_df, "DepartmentID", "right")
right_join_df.show()

# Outer Join (all records from both sides)
print("\nOuter Join (all records from both sides):")
outer_join_df = emp_df.join(dept_df, "DepartmentID", "outer")
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
salary_join = emp_df.join(sal_df, col("EmployeeID") == col("PersonID"), "inner")
salary_join.show()

print("\nLeft Join: all employees, matching salaries:")
salary_left_join = emp_df.join(sal_df, col("EmployeeID") == col("PersonID"), "left")
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
    (col("proj_df.EmployeeID") == col("assign_df.EmployeeID")) & 
    (col("proj_df.DepartmentID") == col("assign_df.DepartmentID")),
    "inner"
)
multi_join.show()

# Alternative: using list of column names (when names match)
print("\nLeft Join on multiple fields (using implicit names):")
multi_left_join = proj_df.join(
    assign_df,
    ["EmployeeID", "DepartmentID"],
    "left"
)
multi_left_join.show()

# --- Full Outer Join with Multiple Conditions ---
print("\nFull Outer Join on multiple fields:")
multi_outer_join = proj_df.join(
    assign_df,
    ["EmployeeID", "DepartmentID"],
    "outer"
)
multi_outer_join.show()

# Create another DataFrame for union
more_employees = [
    (5, "Frank", 104),
]
more_emp_df = spark.createDataFrame(more_employees, emp_columns)

# Union
print("\n--- UNION ---")
print("\nUnion of two employee DataFrames:")
combined_df = emp_df.union(more_emp_df)
combined_df.show()

spark.stop()
