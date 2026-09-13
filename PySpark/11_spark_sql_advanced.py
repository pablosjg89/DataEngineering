"""
Spark SQL - Views, Catalog, Joins, Set Operations, and Query Plans

05_sql_queries.py covers the basics of spark.sql() (SELECT/WHERE/GROUP BY,
window functions, CTEs, subqueries). This file covers what that one doesn't:
- Local vs. global temp views, and how their visibility differs
- The spark.catalog API for inspecting what tables/views/columns exist
- SQL join types (INNER/LEFT/RIGHT/FULL/SEMI/ANTI)
- Set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
- Multi-level aggregation with ROLLUP/CUBE/GROUPING SETS
- Reading a query's execution plan with .explain(), to see the
  Catalyst/Tungsten optimizations DataFrames get that RDDs don't
- Running SQL against a real CSV dataset
"""

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSQLAdvanced") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

employees = [
    ("Alice", "Engineering", "Senior", 95000),
    ("Bob", "Sales", "Junior", 55000),
    ("Charlie", "Engineering", "Junior", 70000),
    ("Diana", "Sales", "Senior", 85000),
    ("Eve", "HR", "Senior", 60000),
    ("Frank", "Engineering", "Senior", 100000),
]
employees_df = spark.createDataFrame(employees, ["Name", "Department", "Level", "Salary"])

# Grace isn't in `employees` and Diana/Eve/Frank have no project - good for
# demonstrating unmatched rows in outer/semi/anti joins
projects = [
    ("Alice", "Recommendation Engine"),
    ("Bob", "Sales Dashboard"),
    ("Charlie", "Recommendation Engine"),
    ("Grace", "Mobile App"),
]
projects_df = spark.createDataFrame(projects, ["Name", "Project"])

print("=" * 80)
print("SPARK SQL - VIEWS, CATALOG, JOINS, SET OPERATIONS, QUERY PLANS")
print("=" * 80)

# ============================================================================
# SECTION 1: TEMP VIEWS - Local vs. global visibility
# ============================================================================
print("\n" + "=" * 80)
print("1. TEMP VIEWS - createOrReplaceTempView() vs createOrReplaceGlobalTempView()")
print("=" * 80)

employees_df.createOrReplaceTempView("employees")
projects_df.createOrReplaceTempView("projects")
employees_df.createOrReplaceGlobalTempView("employees_global")

print("\n--- createOrReplaceTempView() - Scoped to this SparkSession ---")
spark.sql("SELECT Name, Department FROM employees LIMIT 3").show()

print("\n--- createOrReplaceGlobalTempView() - Lives under the 'global_temp' database, ---")
print("--- shared across SparkSessions in the same Spark application ---")
spark.sql("SELECT Name, Department FROM global_temp.employees_global LIMIT 3").show()

new_session = spark.newSession()
print("A new SparkSession CAN see the global temp view:")
new_session.sql("SELECT COUNT(*) AS EmployeeCount FROM global_temp.employees_global").show()

print("But it CANNOT see the plain (session-local) temp view:")
try:
    new_session.sql("SELECT * FROM employees").show()
except Exception as e:
    print(f"  Failed as expected: {type(e).__name__}: {str(e).splitlines()[0]}")

# ============================================================================
# SECTION 2: THE CATALOG API - Inspect what tables/views/columns exist
# ============================================================================
print("\n" + "=" * 80)
print("2. THE CATALOG API - spark.catalog lets you inspect metadata programmatically")
print("=" * 80)

print("\n--- listTables() - All tables/views visible in the current session ---")
for table in spark.catalog.listTables():
    print(f"  {table.name} (temporary={table.isTemporary})")

print("\n--- listColumns() - Column names and types for one table ---")
for column in spark.catalog.listColumns("employees"):
    print(f"  {column.name}: {column.dataType}")

print("\n--- tableExists() - Check for a table/view before querying it ---")
print(f"'employees' exists: {spark.catalog.tableExists('employees')}")
print(f"'nonexistent_table' exists: {spark.catalog.tableExists('nonexistent_table')}")

# ============================================================================
# SECTION 3: SQL JOINS - INNER, LEFT, RIGHT, FULL, SEMI, ANTI
# ============================================================================
print("\n" + "=" * 80)
print("3. SQL JOINS - INNER, LEFT, RIGHT, FULL, LEFT SEMI, LEFT ANTI")
print("=" * 80)

print("\n--- INNER JOIN - Only employees that have a project ---")
spark.sql("""
    SELECT e.Name, e.Department, p.Project
    FROM employees e
    JOIN projects p ON e.Name = p.Name
""").show()

print("\n--- LEFT JOIN - All employees, project is NULL if they don't have one ---")
spark.sql("""
    SELECT e.Name, e.Department, p.Project
    FROM employees e
    LEFT JOIN projects p ON e.Name = p.Name
    ORDER BY e.Name
""").show()

print("\n--- RIGHT JOIN - All projects, employee columns are NULL if unmatched (Grace) ---")
spark.sql("""
    SELECT e.Name, e.Department, p.Project
    FROM employees e
    RIGHT JOIN projects p ON e.Name = p.Name
""").show()

print("\n--- FULL OUTER JOIN - Every row from both sides, matched where possible ---")
spark.sql("""
    SELECT e.Name AS EmployeeName, p.Name AS ProjectOwner, p.Project
    FROM employees e
    FULL OUTER JOIN projects p ON e.Name = p.Name
    ORDER BY EmployeeName
""").show()

print("\n--- LEFT SEMI JOIN - Employees that HAVE a project (columns from left side only) ---")
spark.sql("""
    SELECT e.Name, e.Department
    FROM employees e
    LEFT SEMI JOIN projects p ON e.Name = p.Name
""").show()

print("\n--- LEFT ANTI JOIN - Employees that DON'T have a project ---")
spark.sql("""
    SELECT e.Name, e.Department
    FROM employees e
    LEFT ANTI JOIN projects p ON e.Name = p.Name
""").show()

# ============================================================================
# SECTION 4: SET OPERATIONS - UNION, UNION ALL, INTERSECT, EXCEPT
# ============================================================================
print("\n" + "=" * 80)
print("4. SET OPERATIONS - UNION, UNION ALL, INTERSECT, EXCEPT")
print("=" * 80)

print("\n--- UNION - Departments with a Senior, combined with departments with a Junior ---")
print("--- (duplicates removed) ---")
spark.sql("""
    SELECT Department FROM employees WHERE Level = 'Senior'
    UNION
    SELECT Department FROM employees WHERE Level = 'Junior'
""").orderBy("Department").show()

print("\n--- UNION ALL - Same query, but duplicates are kept ---")
spark.sql("""
    SELECT Department FROM employees WHERE Level = 'Senior'
    UNION ALL
    SELECT Department FROM employees WHERE Level = 'Junior'
""").orderBy("Department").show()

print("\n--- INTERSECT - Departments that have BOTH a Senior and a Junior ---")
spark.sql("""
    SELECT Department FROM employees WHERE Level = 'Senior'
    INTERSECT
    SELECT Department FROM employees WHERE Level = 'Junior'
""").show()

print("\n--- EXCEPT - Departments with a Senior but NO Junior ---")
spark.sql("""
    SELECT Department FROM employees WHERE Level = 'Senior'
    EXCEPT
    SELECT Department FROM employees WHERE Level = 'Junior'
""").show()

# ============================================================================
# SECTION 5: MULTI-LEVEL AGGREGATION - ROLLUP, CUBE, GROUPING SETS
# ============================================================================
print("\n" + "=" * 80)
print("5. MULTI-LEVEL AGGREGATION - ROLLUP, CUBE, GROUPING SETS")
print("=" * 80)

print("\n--- ROLLUP - Subtotals per Department, then a grand total (NULL, NULL row) ---")
spark.sql("""
    SELECT Department, Level, SUM(Salary) AS TotalSalary
    FROM employees
    GROUP BY ROLLUP(Department, Level)
    ORDER BY Department, Level
""").show()

print("\n--- CUBE - Every combination of subtotals: by Department, by Level, and both ---")
spark.sql("""
    SELECT Department, Level, SUM(Salary) AS TotalSalary
    FROM employees
    GROUP BY CUBE(Department, Level)
    ORDER BY Department, Level
""").show()

print("\n--- GROUPING SETS - Pick exactly the subtotal combinations you want ---")
spark.sql("""
    SELECT Department, Level, SUM(Salary) AS TotalSalary
    FROM employees
    GROUP BY GROUPING SETS ((Department), (Level), ())
    ORDER BY Department, Level
""").show()

# ============================================================================
# SECTION 6: QUERY PLANS - .explain() shows Catalyst's optimized plan
# ============================================================================
print("\n" + "=" * 80)
print("6. QUERY PLANS - .explain() reveals the Catalyst-optimized execution plan")
print("=" * 80)

print("\n--- explain() on a filtered, aggregated query ---")
print("This is the kind of plan the Catalyst optimizer builds automatically for")
print("DataFrames/SQL - RDDs get no equivalent optimization pass:")
spark.sql("""
    SELECT Department, AVG(Salary) AS AvgSalary
    FROM employees
    WHERE Salary > 60000
    GROUP BY Department
""").explain()

# ============================================================================
# SECTION 7: SQL AGAINST A REAL DATASET
# ============================================================================
print("\n" + "=" * 80)
print("7. SQL AGAINST A REAL DATASET - spark.read.csv() + spark.sql()")
print("=" * 80)

dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Datasets", "melb_data.csv")
melb_df = spark.read.csv(dataset_path, header=True, inferSchema=True)
melb_df.createOrReplaceTempView("melbourne_housing")

print("\n--- Average price and listing count by region ---")
spark.sql("""
    SELECT Regionname, COUNT(*) AS Listings, ROUND(AVG(Price), 2) AS AvgPrice
    FROM melbourne_housing
    WHERE Price IS NOT NULL
    GROUP BY Regionname
    ORDER BY AvgPrice DESC
""").show(truncate=False)

print("\n--- Top 5 most expensive suburbs with at least 20 listings ---")
spark.sql("""
    SELECT Suburb, COUNT(*) AS Listings, ROUND(AVG(Price), 2) AS AvgPrice
    FROM melbourne_housing
    WHERE Price IS NOT NULL
    GROUP BY Suburb
    HAVING COUNT(*) >= 20
    ORDER BY AvgPrice DESC
    LIMIT 5
""").show(truncate=False)

print("\n" + "=" * 80)
print("END OF SPARK SQL ADVANCED EXAMPLES")
print("=" * 80)

spark.stop()
