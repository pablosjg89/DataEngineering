"""
Joins and Unions with PySpark
Examples of combining multiple DataFrames.
"""

from pyspark.sql import SparkSession

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

# Inner Join
print("\nInner Join (Employees with Departments):")
joined_df = emp_df.join(dept_df, "DepartmentID")
joined_df.show()

# Left Join
print("\nLeft Join:")
left_join_df = emp_df.join(dept_df, "DepartmentID", "left")
left_join_df.show()

# Create another DataFrame for union
more_employees = [
    (4, "David", 102),
]
more_emp_df = spark.createDataFrame(more_employees, emp_columns)

# Union
print("\nUnion of two employee DataFrames:")
combined_df = emp_df.union(more_emp_df)
combined_df.show()

spark.stop()
