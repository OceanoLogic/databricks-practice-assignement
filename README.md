# DataBricks Practice Assignment

## Data Cleaning and Standardization

1. Create a PySpark DataFrame manually with sample data:

data = [
    ("Alice ", " IT", 45000, "2021-05-10"),
    (" bob", "Finance", None, "2020-12-01"),
    ("ALICE", " it ", 50000, "2021-05-10"),
    ("John", "HR", 40000, "2022-01-15"),
]

Columns: name, department, salary, joining_date.

2. Perform the following cleaning:

Trim spaces in name and department.

Convert all name values to Proper Case.

Fill missing salary values with department average.

Drop duplicates.

3. Add a new column joining_year extracted from joining_date.

4. Show before and after cleaning results.

Delta Table Versioning and Rollback

1. Create a small DataFrame:
data = [(1, "Apple", 100), (2, "Banana", 50), (3, "Mango", 80)]
columns = ["product_id", "product_name", "stock"]

2. Write it as a Delta table: inventory.

3. Perform the following:

Update stock of Mango to 120.

Delete Banana record.

Insert a new record (4, "Orange", 70).

4. Check your version and Restore table to an earlier version.

## Grouping and Aggregation Challenge

1. Create a DataFrame:
data = [
    ("North", "ProductA", 200),
    ("South", "ProductA", 150),
    ("East", "ProductB", 300),
    ("West", "ProductB", 100),
    ("North", "ProductB", 400),
]
columns = ["region", "product", "sales"]

2. Calculate:

Total sales per product.

Average sales per region.

Top product per region using window functions.

3. Display results.


Dataframe Joins and Enrichment

1. Create two DataFrames:

customers = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
orders = [(1, 200), (1, 100), (2, 150), (4, 300)]
Columns:

Customers: cust_id, cust_name

Orders: cust_id, order_value

2. Perform:

Inner join

Left join

Full outer join

Anti join (to find customers with no orders)

3. Handle nulls by filling with “Unknown”.

4. Compute total order value per customer.

## Data Types (Structs, Arrays)

1. Create a DataFrame:

data = [
    ("A1", ["apple", "banana"]),
    ("A2", ["orange", "apple"]),
    ("A3", ["grape"]),
]
columns = ["order_id", "items"]

2. Perform:

Add a new item to the array column (append “watermelon”).

Explode the array to individual rows.

Count how many times each fruit appears.

3. Create a nested struct column like: withColumn("order_info", struct(col("order_id"), col("items")))

4. Query nested fields.

## Daily Incremental Load Script

Task:
Write a generic PySpark script that loads data from a storage location (e.g: ADLS) into Delta tables incrementally every day for 3 different source tables — customers, orders, and products.

Requirements:

- Load only new or changed records based on a timestamp or ID column.
- Maintain separate checkpoints or metadata for each table to track last load.
- Merge incremental data into existing Delta tables.
- Validate row counts and duplicates after each load.
- Log all ETL activities with timestamps and status (SUCCESS/FAILURE).