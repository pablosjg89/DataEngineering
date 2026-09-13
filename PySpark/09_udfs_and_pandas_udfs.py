"""
PySpark UDFs and Pandas UDFs

PySpark UDFs (regular, row-at-a-time UDFs):
- Wrap a plain Python function with udf() and it's immediately usable in
  DataFrame expressions across every node in the Spark session - Spark ships
  the function to the executors for you, no registration needed. Registering
  with spark.udf.register() is only required to call it from a spark.sql()
  string query.
- Operate one row at a time
- Each row is serialized from the JVM to a Python worker process and back
- Simple to write, but slow for large datasets due to per-row serialization overhead

Pandas UDFs (vectorized UDFs):
- Operate on batches of rows as pandas Series/DataFrames, using Apache Arrow
  to transfer data between the JVM and Python in columnar batches
- Much faster than PySpark UDFs because pandas/NumPy operations are vectorized
  and serialization overhead is amortized across a whole batch, not per row
- Require pyarrow to be installed (bundled with pyspark-env)

Rule of thumb: prefer built-in pyspark.sql.functions first (fastest, no
serialization at all). For bigger DataFrames, prefer a Pandas UDF - the
vectorized, Arrow-backed execution scales much better than row-at-a-time
calls. Reach for a plain PySpark UDF when convenience matters more than raw
throughput (small/medium data, or logic that doesn't vectorize well), since
it works immediately across the whole cluster with no extra setup.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import DoubleType, StringType

spark = SparkSession.builder \
    .appName("UDFsAndPandasUDFs") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

data = [
    ("Alice", "Engineering", 80000),
    ("Bob", "Sales", 65000),
    ("Charlie", "Engineering", 75000),
    ("Diana", "Sales", 70000),
    ("Eve", "HR", 55000),
]
columns = ["Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

print("=" * 80)
print("PYSPARK UDFs AND PANDAS UDFs")
print("=" * 80)

print("\nOriginal DataFrame:")
df.show()

# ============================================================================
# SECTION 1: PYSPARK UDF (REGULAR, ROW-AT-A-TIME)
# ============================================================================
print("\n" + "=" * 80)
print("1. PYSPARK UDF - Operates on one row's value at a time")
print("=" * 80)


def salary_bracket(salary):
    if salary >= 75000:
        return "High"
    elif salary >= 65000:
        return "Medium"
    else:
        return "Low"


# Wrap the plain Python function as a UDF, declaring its return type
salary_bracket_udf = udf(salary_bracket, StringType())

print("\n--- udf() - Wrap a Python function to use in DataFrame expressions ---")
print("Salary bracket per employee (computed row by row):")
df.select(
    col("Name"),
    col("Salary"),
    salary_bracket_udf(col("Salary")).alias("SalaryBracket")
).show()

# Same UDF, registered for use in spark.sql() queries
spark.udf.register("salary_bracket_sql", salary_bracket, StringType())

print("\n--- spark.udf.register() - Use the same UDF from SQL ---")
df.createOrReplaceTempView("employees")
spark.sql("""
    SELECT Name, Salary, salary_bracket_sql(Salary) AS SalaryBracket
    FROM employees
""").show()

# ============================================================================
# SECTION 2: PANDAS UDF (SCALAR) - Vectorized, batch-at-a-time
# ============================================================================
print("\n" + "=" * 80)
print("2. PANDAS UDF (SCALAR) - Operates on a whole batch (pandas Series) at once")
print("=" * 80)


@pandas_udf(DoubleType())
def bonus_pct_of_salary(salary: pd.Series) -> pd.Series:
    # Vectorized pandas/NumPy math - no per-row Python function calls
    return (salary * 0.10).round(2)


print("\n--- @pandas_udf - Vectorized column transformation ---")
print("10% bonus computed for the whole column in one batch call:")
df.select(
    col("Name"),
    col("Salary"),
    bonus_pct_of_salary(col("Salary")).alias("Bonus")
).show()

# ============================================================================
# SECTION 3: PANDAS UDF (GROUPED MAP) - applyInPandas
# ============================================================================
print("\n" + "=" * 80)
print("3. GROUPED MAP PANDAS UDF - Apply a pandas function per group")
print("=" * 80)


def zscore_salary(pdf: pd.DataFrame) -> pd.DataFrame:
    # pdf holds all rows for one Department as a local pandas DataFrame
    pdf["SalaryZScore"] = (pdf["Salary"] - pdf["Salary"].mean()) / pdf["Salary"].std(ddof=0)
    return pdf


result_schema = "Name string, Department string, Salary long, SalaryZScore double"

print("\n--- applyInPandas() - Per-department salary z-score ---")
print("Each department's group is handed to zscore_salary() as its own pandas DataFrame:")
print("(HR has a single employee, so its std-dev is 0 -> z-score is NULL, as expected)")
df.groupBy("Department").applyInPandas(zscore_salary, schema=result_schema).orderBy("Department", "Name").show()

print("\n" + "=" * 80)
print("END OF PYSPARK UDFs AND PANDAS UDFs EXAMPLES")
print("=" * 80)

spark.stop()
