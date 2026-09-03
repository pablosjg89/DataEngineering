"""
SQL Queries with PySpark
Examples of using Spark SQL to query DataFrames.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Create a DataFrame
data = [
    ("Alice", 25, "Sales", 50000),
    ("Bob", 30, "Engineering", 80000),
    ("Charlie", 35, "Sales", 60000),
    ("David", 28, "Engineering", 75000),
    ("Eve", 32, "HR", 55000),
]
columns = ["Name", "Age", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# Register DataFrame as a SQL table
df.createOrReplaceTempView("employees")

print("All Employees:")
spark.sql("SELECT * FROM employees").show()

print("\nEngineering Department:")
spark.sql("SELECT Name, Salary FROM employees WHERE Department = 'Engineering'").show()

print("\nAverage Salary by Department:")
spark.sql("""
    SELECT Department, AVG(Salary) as AvgSalary
    FROM employees
    GROUP BY Department
    ORDER BY AvgSalary DESC
""").show()

print("\nEmployees over 30 years old:")
spark.sql("SELECT Name, Age, Salary FROM employees WHERE Age > 30").show()

print("\nTop earner by department:")
spark.sql("""
    SELECT Department, MAX(Salary) as MaxSalary
    FROM employees
    GROUP BY Department
""").show()

spark.stop()
