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
df.write.format("delta").mode("overwrite").saveAsTable("my_delta_table")
my_delta_table = spark.read.format("delta").table("my_delta_table")

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
df_inserted.write.format("delta").saveAsTable("my_delta_table", mode="overwrite")
spark.sql("DESCRIBE HISTORY my_delta_table").show(truncate=False)
df_restored = (
    spark.read.format("delta").option("versionAsOf", 0).table("my_delta_table")
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
df = spark.createDataFrame(data, columns)


# 2. Calculate:

# Total sales per product.
df_total_sales = df.groupBy("product").agg(F.sum("sales").alias("total_sales"))

# Average sales per region.
df_avg_sales = df.groupBy("region").agg(F.sum("sales").alias("avg_sales"))

# Top product per region using window functions.
windowSpec = Window.partitionBy("region").orderBy(F.desc("sales"))
df_top_product = df.withColumn("rank", F.rank().over(windowSpec)).alias("top_product")

# 3. Display results.
df_total_sales.show()
df_avg_sales.show()
df_top_product.show()

# Dataframe Joins and Enrichment

# 1. Create two DataFrames:
customers = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
orders = [(1, 200), (1, 100), (2, 150), (4, 300)]

# Columns:

# Customers: cust_id, cust_name
cols_customers = ["cust_id", "cust_name"]

# Orders: cust_id, order_value
col_orders = ["cust_id", "order_value"]

df_customers = spark.createDataFrame(customers, cols_customers)
df_orders = spark.createDataFrame(orders, col_orders)

# 2. Perform:

# Inner join
df_inner_join = df_customers.join(df_orders, on="cust_id", how="inner")

# Left join
df_left_join = df_customers.join(df_orders, on="cust_id", how="left")


# Full outer join
df_full_outer_join = df_customers.join(df_orders, on="cust_id", how="outer")

# Anti join (to find customers with no orders)
df_anti_join = df_customers.join(df_orders, on="cust_id", how="left_anti")

# 3. Handle nulls by filling with “Unknown”.
df_left_filled_nulls = df_left_join.fillna({"order_value": "Unknown"})
df_full_outer_join = df_customers.join(df_orders, on="cust_id", how="outer")
df_full_outer_filled_nulls = df_full_outer_join.fillna(
    {"cust_name": "Unknown", "order_value": 0}
)
df_full_outer_filled_nulls.show()

# 4. Compute total order value per customer.
df_total_order_value = df_inner_join.groupBy("cust_id", "cust_name").agg(
    F.sum("order_value").alias("total_order_value")
)
df_total_order_value.show()

# Data Types (Structs, Arrays)
# 1. Create a DataFrame:
data = [
    ("A1", ["apple", "banana"]),
    ("A2", ["orange", "apple"]),
    ("A3", ["grape"]),
]
columns = ["order_id", "items"]

df = spark.createDataFrame(data, columns)

# 2. Perform:

# Add a new item to the array column (append “watermelon”).
df_with_new_item = df.withColumn(
    "items", F.array_union(F.col("items"), F.array(F.lit("watermelon")))
)

# Explode the array to individual rows.
df_exploded = df_with_new_item.withColumn("item", F.explode(F.col("items")))

# Count how many times each fruit appears.
df_fruit_count = df_exploded.groupBy("item").agg(F.count("*").alias("count"))

# 3. Create a nested struct column like: withColumn("order_info", struct(col("order_id"), col("items")))
df_nested = df.withColumn(
    "order_info", F.struct(F.col("order_id"), F.col("items"))
).select("order_info")

# 4. Query nested fields.
df_nested.select("order_info.order_id", "order_info.items").show(truncate=False)


# Daily Incremental Load Script

# Task:
# Write a generic PySpark script that loads data from a storage location (e.g: ADLS) into Delta tables incrementally every day for 3 different source tables — customers, orders, and products.

# Requirements:

# Load only new or changed records based on a timestamp or ID column.

# Maintain separate checkpoints or metadata for each table to track last load.

# Merge incremental data into existing Delta tables.

# Validate row counts and duplicates after each load.

# Log all ETL activities with timestamps and status (SUCCESS/FAILURE).
