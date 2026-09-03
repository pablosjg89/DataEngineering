DataEngineering

A collection of data engineering projects, examples, and demonstrations.

## Project Structure

```
DataEngineering/
├── PySpark/            # PySpark examples and tutorials
│   ├── 01_basic_setup.py
│   ├── 02_data_operations.py
│   ├── 03_joins_and_unions.py
│   ├── 04_file_operations.py
│   ├── 05_sql_queries.py
│   ├── 06_structured_formats.py
│   └── README.md
└── README.md
```

## Contents

### PySpark Examples (`PySpark/`)

A comprehensive collection of PySpark examples covering essential data processing operations:

- **01_basic_setup.py** - SparkSession initialization and basic DataFrame creation
- **02_data_operations.py** - Filtering, grouping, and aggregating operations (includes where(), sort(), na.drop(), na.fill(), withColumn(), withColumnRenamed(), drop() examples)
- **03_joins_and_unions.py** - Combining DataFrames using inner/left joins and unions
- **04_file_operations.py** - Reading and writing data in CSV, JSON, and Parquet formats
- **05_sql_queries.py** - Using Spark SQL for advanced querying and analysis
- **06_structured_formats.py** - Reading nested JSON and partitioned Parquet with schema merging

Each example includes runnable code and detailed comments for learning purposes.

### Getting Started

#### Requirements
- Python 3.x
- PySpark

#### Installation

```bash
pip install pyspark
```

#### Running Examples

Navigate to the `PySpark/` directory and run any example:

```bash
cd PySpark
python 01_basic_setup.py
python 02_data_operations.py
python 03_joins_and_unions.py
python 04_file_operations.py
python 05_sql_queries.py
python 06_structured_formats.py
```

## Use Cases

These examples are ideal for:
- Learning PySpark fundamentals
- Quick reference for common operations
- Data processing pipeline development
- Spark SQL query patterns
- Data format handling (CSV, JSON, Parquet)

## About pyspark.sql.functions (alias F)

The module `pyspark.sql.functions` (commonly imported as `F`) provides a set of column expressions and utilities — avg, count, min, max, when, lit, col, concat, explode, and more — which are used throughout the examples for aggregations, expressions, and transformations. Using the `F` alias keeps expressions concise (e.g., `F.avg('Salary')`) and helps avoid naming conflicts with local variables.

## License

This project is open source and available for learning and reference purposes.

## Contributing

Feel free to expand this collection with additional examples or improvements!
