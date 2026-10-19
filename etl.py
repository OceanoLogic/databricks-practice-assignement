import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql.window import Window

# Data Cleaning and Standardization
# 1. Create a PySpark DataFrame manually with sample data:

data = [
    ("Alice ", " IT", 45000, "2021-05-10"),
    (" bob", "Finance", None, "2020-12-01"),
    ("ALICE", " it ", 50000, "2021-05-10"),
    ("John", "HR", 40000, "2022-01-15"),
]

spark = DatabricksSession.builder.serverless(True).getOrCreate()

# Columns: name, department, salary, joining_date.
df = spark.createDataFrame(data, ["name", "department", "salary", "joining_date"])

# 2. Perform the following cleaning:

# Trim spaces in name and department.
df_cleaned = df.withColumn("name", F.trim(F.col("name"))).withColumn(
    "department", F.trim(F.col("department"))
)


# Convert all name values to Proper Case.
df_proper = df_cleaned.withColumn("name", F.initcap(F.col("name")))

# Fill missing salary values with department average.
df_filled = df_proper.na.fill({"salary": df_proper.select(F.mean("salary")).first()[0]})

# Drop duplicates.
df_unique = df_filled.dropDuplicates()

# 3. Add a new column joining_year extracted from joining_date.
df_year = df_unique.withColumn(
    "joining_year", F.year(F.to_date(F.col("joining_date"), "yyyy-MM-dd"))
)

# 4. Show before and after cleaning results.
df.show(truncate=False)
df_year.show(truncate=False)


# Delta Table Versioning and Rollback

# 1. Create a small DataFrame:
data = [(1, "Apple", 100), (2, "Banana", 50), (3, "Mango", 80)]
columns = ["product_id", "product_name", "stock"]


# 2. Write it as a Delta table: inventory.
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").save("/tmp/delta/inventory")
# 3. Perform the following:

# Update stock of Mango to 120.
df_updated = df.withColumn(
    "stock", F.when(F.col("product_name") == "Mango", 120).otherwise(F.col("stock"))
)
# Delete Banana record.
df_filtered = df_updated.filter(F.col("product_name") != "Banana")

# Insert a new record (4, "Orange", 70).
df_inserted = df_filtered.union(spark.createDataFrame([(4, "Orange", 70)], columns))

# 4. Check your version and Restore table to an earlier version.
df_inserted.write.format("delta").mode("overwrite").save("/tmp/delta/inventory")
version = df_inserted.selectExpr("MAX(_commit_version) -1 as previous_version").show()
df_restored = (
    spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/inventory")
)
df_restored.show(truncate=False)


# Grouping and Aggregation Challenge

# 1. Create a DataFrame:
data = [
    ("North", "ProductA", 200),
    ("South", "ProductA", 150),
    ("East", "ProductB", 300),
    ("West", "ProductB", 100),
    ("North", "ProductB", 400),
]
columns = ["region", "product", "sales"]


# 2. Calculate:

# Total sales per product.

# Average sales per region.

# Top product per region using window functions.

# 3. Display results.


# Dataframe Joins and Enrichment

# 1. Create two DataFrames:
customers = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
orders = [(1, 200), (1, 100), (2, 150), (4, 300)]

# Columns:

# Customers: cust_id, cust_name

# Orders: cust_id, order_value

# 2. Perform:

# Inner join

# Left join

# Full outer join

# Anti join (to find customers with no orders)

# 3. Handle nulls by filling with “Unknown”.

# 4. Compute total order value per customer.

# Data Types (Structs, Arrays)
# 1. Create a DataFrame:
data = [
    ("A1", ["apple", "banana"]),
    ("A2", ["orange", "apple"]),
    ("A3", ["grape"]),
]
columns = ["order_id", "items"]
# 2. Perform:

# Add a new item to the array column (append “watermelon”).

# Explode the array to individual rows.

# Count how many times each fruit appears.

# 3. Create a nested struct column like: withColumn("order_info", struct(col("order_id"), col("items")))

# 4. Query nested fields.


# Daily Incremental Load Script

# Task:
# Write a generic PySpark script that loads data from a storage location (e.g: ADLS) into Delta tables incrementally every day for 3 different source tables — customers, orders, and products.

# Requirements:

# Load only new or changed records based on a timestamp or ID column.

# Maintain separate checkpoints or metadata for each table to track last load.

# Merge incremental data into existing Delta tables.

# Validate row counts and duplicates after each load.

# Log all ETL activities with timestamps and status (SUCCESS/FAILURE).
