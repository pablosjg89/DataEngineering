"""
SQL Queries with PySpark - Including ipython-sql Magic

PySpark SQL allows querying DataFrames using standard SQL syntax.
This file demonstrates:
1. Traditional Spark SQL with spark.sql()
2. IPython Magic SQL (for Jupyter notebooks) with %sql
3. Query patterns and best practices

About ipython-sql magic (%sql):
- Enables direct SQL execution in Jupyter notebooks
- Syntax: %sql <query> or %%sql for multi-line queries
- Automatically captures results as variables
- Integrates with both PySpark and pandas
- Makes notebooks more interactive and readable

Installation (for Jupyter):
    pip install ipython-sql sqlalchemy
    
Usage in Jupyter:
    %load_ext sql
    %sql SELECT * FROM table_name
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

print("=" * 80)
print("PYSPARK SQL QUERIES - TRADITIONAL AND MAGIC SYNTAX")
print("=" * 80)

# ============================================================================
# SECTION 1: TRADITIONAL SPARK SQL (works in scripts and notebooks)
# ============================================================================
print("\n" + "=" * 80)
print("1. TRADITIONAL SPARK SQL - spark.sql() method")
print("=" * 80)

print("\n--- All Employees ---")
spark.sql("SELECT * FROM employees").show()

print("\n--- Engineering Department ---")
spark.sql("SELECT Name, Salary FROM employees WHERE Department = 'Engineering'").show()

print("\n--- Average Salary by Department ---")
spark.sql("""
    SELECT Department, AVG(Salary) as AvgSalary
    FROM employees
    GROUP BY Department
    ORDER BY AvgSalary DESC
""").show()

print("\n--- Employees over 30 years old ---")
spark.sql("SELECT Name, Age, Salary FROM employees WHERE Age > 30").show()

print("\n--- Top earner by department ---")
spark.sql("""
    SELECT Department, MAX(Salary) as MaxSalary
    FROM employees
    GROUP BY Department
""").show()

# ============================================================================
# SECTION 2: IPYTHON MAGIC SQL (for Jupyter notebooks)
# ============================================================================
print("\n" + "=" * 80)
print("2. IPYTHON MAGIC SQL - For use in Jupyter notebooks")
print("=" * 80)

print("""
JUPYTER NOTEBOOK EXAMPLES:
(Note: These work in Jupyter, not in this script)

Step 1: Load the SQL magic extension in first cell
    %load_ext sql
    %sql sqlite:///

Step 2: Single-line magic queries
    %sql SELECT * FROM employees
    %sql SELECT Name, Salary FROM employees WHERE Salary > 60000
    %sql SELECT COUNT(*) as EmployeeCount FROM employees

Step 3: Multi-line magic queries with %%sql
    %%sql
    SELECT 
        Department, 
        COUNT(*) as Count,
        AVG(Salary) as AvgSalary,
        MAX(Salary) as MaxSalary
    FROM employees
    GROUP BY Department
    ORDER BY AvgSalary DESC

Step 4: Capture results in variables
    results = %sql SELECT Name, Salary FROM employees
    df_results = results.DataFrame  # Convert to pandas DataFrame
    print(df_results)

Step 5: Use variables in queries
    min_salary = 70000
    %sql SELECT Name, Salary FROM employees WHERE Salary >= :min_salary
""")

# ============================================================================
# SECTION 3: ADVANCED SQL PATTERNS (applicable to both approaches)
# ============================================================================
print("\n" + "=" * 80)
print("3. ADVANCED SQL PATTERNS")
print("=" * 80)

# Window Functions with SQL
print("\n--- Window Functions (Ranking within department) ---")
spark.sql("""
    SELECT 
        Name,
        Department,
        Salary,
        ROW_NUMBER() OVER (PARTITION BY Department ORDER BY Salary DESC) as SalaryRank
    FROM employees
""").show()

# Common Table Expressions (CTE)
print("\n--- Common Table Expressions (CTE) ---")
spark.sql("""
    WITH dept_stats AS (
        SELECT 
            Department,
            AVG(Salary) as AvgDeptSalary,
            COUNT(*) as DeptSize
        FROM employees
        GROUP BY Department
    )
    SELECT 
        e.Name,
        e.Department,
        e.Salary,
        d.AvgDeptSalary,
        CASE 
            WHEN e.Salary > d.AvgDeptSalary THEN 'Above Average'
            WHEN e.Salary = d.AvgDeptSalary THEN 'At Average'
            ELSE 'Below Average'
        END as SalaryStatus
    FROM employees e
    JOIN dept_stats d ON e.Department = d.Department
    ORDER BY e.Department, e.Salary DESC
""").show()

# Subqueries
print("\n--- Subqueries ---")
spark.sql("""
    SELECT Name, Salary
    FROM employees
    WHERE Salary > (SELECT AVG(Salary) FROM employees)
    ORDER BY Salary DESC
""").show()

# ============================================================================
# SECTION 4: IPYTHON-SQL LIBRARY FEATURES (if available)
# ============================================================================
print("\n" + "=" * 80)
print("4. IPYTHON-SQL LIBRARY FEATURES (Jupyter Integration)")
print("=" * 80)

print("""
When using ipython-sql in Jupyter, you get these advantages:

1. Direct SQL Execution:
   - Write SQL directly without spark.sql() wrapper
   - Cleaner, more readable notebooks
   
2. Result Display:
   - Pretty-printed table output
   - Automatic truncation for large result sets
   - Easy result export to pandas DataFrame
   
3. Performance Monitoring:
   - %time to measure query execution time
   - %%time for multi-line queries
   - Built-in profiling information
   
4. Query Reusability:
   - Store results in variables
   - Reference previous results in new queries
   - Build complex queries incrementally
   
5. Database Agnostic:
   - Works with multiple SQL databases
   - Can connect to Spark, SQLite, PostgreSQL, MySQL, etc.
   - Same syntax across different databases

Example Jupyter Workflow:

    # Load magic extension
    %load_ext sql
    
    # Connect to database
    %sql sqlite:///
    
    # Quick query with timing
    %time %sql SELECT COUNT(*) FROM employees
    
    # Multi-line query
    %%sql
    SELECT Department, AVG(Salary) as AvgSalary
    FROM employees
    GROUP BY Department
    ORDER BY AvgSalary DESC
    
    # Capture results
    results = %sql SELECT * FROM employees
    df = results.DataFrame
    print(df.describe())
""")

# ============================================================================
# SECTION 5: COMPARISON - WHEN TO USE EACH APPROACH
# ============================================================================
print("\n" + "=" * 80)
print("5. WHEN TO USE EACH APPROACH")
print("=" * 80)

print("""
Traditional spark.sql():
✓ Production scripts and batch jobs
✓ Programmatic query construction
✓ Complex logic with loops/conditionals
✓ Works in Python scripts
✓ Better for error handling in code
✓ No Jupyter dependency

IPython Magic SQL (%sql):
✓ Jupyter notebooks for exploration
✓ Interactive data analysis
✓ Quick ad-hoc queries
✓ Cleaner, more readable notebook cells
✓ Easy result visualization
✓ Great for data scientists and analysts
✓ Fast prototyping and experimentation

Recommendation:
- Use magic SQL (%sql) for Jupyter notebooks and exploration
- Use spark.sql() for production code and scripts
- Learn both for maximum flexibility
""")

spark.stop()
