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
