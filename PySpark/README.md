# PySpark Examples

This directory contains a collection of PySpark examples for learning and reference.

## Files

1. **01_basic_setup.py** - Basic PySpark Setup and Initialization
   - Creating a SparkSession
   - Building a simple DataFrame
   - Displaying schema and data

2. **02_data_operations.py** - Data Operations with PySpark
   - Filtering DataFrames
   - Grouping and aggregating data
   - Selecting specific columns
   - Includes examples for where(), sort(), na.drop(), and na.fill() (null handling)
   - Column operations: withColumn(), withColumnRenamed(), drop() (derived metrics, renaming, and pruning)

3. **03_joins_and_unions.py** - Joins and Unions with PySpark
   - Inner, left, right, and outer join examples
   - Joins with different column names (explicit conditions)
   - Multiple field joins (composite keys)
   - Union operations to combine DataFrames

4. **04_file_operations.py** - File Operations with PySpark
   - Reading and writing CSV files
   - Reading and writing Parquet files
   - Reading and writing JSON files

5. **05_sql_queries.py** - SQL Queries with PySpark
   - Registering DataFrames as SQL tables
   - Running SQL queries on DataFrames
   - Aggregation and filtering using SQL

6. **06_structured_formats.py** - Structured data formats (nested JSON, partitioned Parquet)
   - Reading nested JSON and extracting fields
   - Writing and reading partitioned Parquet with schema merging and partition filters

7. **07_functions_library.py** - Understanding pyspark.sql.functions (F library)
   - Column functions (col, lit, concat)
   - String functions (upper, lower, length, substring)
   - Conditional functions (when, otherwise, coalesce)
   - Aggregation functions (avg, sum, count, min, max, stddev)
   - Date functions (current_date, date_add, datediff)
   - Window functions (row_number, rank, lag, lead)
   - Why to use the F alias and how to compose operations

8. **08_arrays_and_maps.py** - Complex Data Types: Arrays and Maps
   - Creating and manipulating arrays
   - Array operations: contains, size, join, distinct, union
   - Creating and working with maps (key-value pairs)
   - Map operations: keys, values, concat
   - Array of structs for nested data
   - Exploding nested structures
   - Practical use cases for semi-structured data

9. **09_udfs_and_pandas_udfs.py** - PySpark UDFs and Pandas UDFs
   - PySpark UDFs (regular, row-at-a-time) with `udf()` and `spark.udf.register()`
   - A multi-column UDF (several fields passed as separate arguments in one call)
   - Why declaring the return type (`StringType()`, etc.) matters: Spark can't infer
     it from a Python function running in a separate worker process, and a wrong or
     missing type silently produces corrupted columns instead of an error
   - A UDF returning several different types at once via a `StructType` schema
     (a tuple of `(str, float, bool)` mapped to `StringType`/`DoubleType`/`BooleanType`
     fields) - `udf()` always takes exactly one `returnType`, never several positional ones
   - Scalar Pandas UDFs with `@pandas_udf` for vectorized, Arrow-backed transforms
   - Grouped-map Pandas UDFs with `applyInPandas()` for per-group pandas logic
   - When to reach for built-in functions vs. a PySpark UDF vs. a Pandas UDF
   - Tradeoff: Pandas UDFs scale better on bigger DataFrames (vectorized, Arrow-backed);
     PySpark UDFs are more convenient since `udf()` works across every node in the
     Spark session immediately, with no registration step required

10. **10_rdds.py** - RDDs (Resilient Distributed Datasets)
    - DataFrames vs. RDDs: RDDs give maximum low-level flexibility (and preserve
      Python data types) at the cost of more code and no Catalyst/Tungsten
      optimizations; DataFrames trade some of that flexibility for a schema-aware,
      SQL-like high-level API that does more with less code
    - Creating RDDs with `sc.parallelize()` and inspecting partitions
    - Transformations: `map()`, `filter()`, `flatMap()`, `distinct()`
    - Actions: `collect()`, `count()`, `first()`, `take()`, `reduce()`
    - Pair RDD operations: `reduceByKey()`, `groupByKey()`, `mapValues()`, `sortByKey()`
    - Set-like operations: `union()`, `intersection()`, `subtract()`
    - Persistence with `cache()` / `unpersist()` to avoid recomputing an RDD
    - Converting between RDDs and DataFrames with `toDF()` and `df.rdd`
    - Real-dataset example: `spark.read.csv()` on `Datasets/melb_data.csv`, converted
      to an RDD via `.rdd` and aggregated with `map()`/`reduceByKey()`
    - When to reach for the RDD API instead of DataFrames

11. **11_spark_sql_advanced.py** - Spark SQL: Views, Catalog, Joins, Set Operations, Query Plans
    - Builds on 05_sql_queries.py's basics with topics that one doesn't cover
    - Local vs. global temp views (`createOrReplaceTempView()` vs
      `createOrReplaceGlobalTempView()`) and how their visibility differs across
      `SparkSession`s
    - The `spark.catalog` API: `listTables()`, `listColumns()`, `tableExists()`
    - SQL join types: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `LEFT SEMI`, `LEFT ANTI`
    - Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
    - Multi-level aggregation: `ROLLUP`, `CUBE`, `GROUPING SETS`
    - Reading a query's execution plan with `.explain()` - the Catalyst
      optimization DataFrames/SQL get that RDDs don't
    - Running SQL against a real dataset (`Datasets/melb_data.csv`)

## About pyspark.sql.functions (alias F)

Many examples import pyspark.sql.functions and alias it as `F` (for example: `from pyspark.sql import functions as F`). This module provides essential SQL-style functions including:

- **Column functions**: col, lit, concat, coalesce
- **String functions**: upper, lower, length, substring, trim
- **Conditional functions**: when, otherwise
- **Aggregation functions**: avg, count, sum, min, max, stddev
- **Date functions**: current_date, date_add, datediff
- **Window functions**: row_number, rank, dense_rank, lag, lead

Using the `F` alias keeps code concise (e.g., `F.avg('col')`) and clear that these are Spark functions. It also avoids name collisions with Python built-ins (like `sum()`). When writing multiple expressions or aggregations, prefer `F.*` to improve readability and consistency.

See **07_functions_library.py** for comprehensive examples and explanations of all major function categories.

## Running the Examples

To run any example (requires PySpark to be installed):

```bash
python 01_basic_setup.py
python 02_data_operations.py
python 03_joins_and_unions.py
python 04_file_operations.py
python 05_sql_queries.py
python 06_structured_formats.py
python 07_functions_library.py
python 08_arrays_and_maps.py
python 09_udfs_and_pandas_udfs.py
python 10_rdds.py
python 11_spark_sql_advanced.py
```

## Notes

- Examples use `local[*]` mode, which runs in local mode using all available cores
- Log level is set to WARN to reduce output verbosity
- Most examples use simple in-memory data for demonstration purposes
- File operations use temporary directories for demo purposes
- `02_data_operations.py` includes null-handling examples (na.drop, na.fill, where/isNotNull) and sorting examples (orderBy, sort)

## Requirements

- Python 3.x
- PySpark

Install PySpark using pip:
```bash
pip install pyspark
```

## Notes

- Examples use `local[*]` mode, which runs in local mode using all available cores
- Log level is set to WARN to reduce output verbosity
- Most examples use simple in-memory data for demonstration purposes
- File operations use temporary directories for demo purposes
