from datetime import datetime


def generate_example_data(spark):
    """
    customers: cust_id, cust_name, cust_email, created_at
    orders: order_id, cust_id, order_value, order_date
    products: product_id, product_name, price, created_at
    """

    customers = [
        {
            "cust_id": 1,
            "cust_name": "Alice",
            "cust_email": "alice@example.com",
            "created_at": datetime.strptime("2023-01-01", "%Y-%m-%d"),
        },
        {
            "cust_id": 2,
            "cust_name": "Bob",
            "cust_email": "bob@example.com",
            "created_at": datetime.strptime("2023-01-02", "%Y-%m-%d"),
        },
    ]
    orders = [
        {
            "order_id": 1,
            "cust_id": 1,
            "order_value": 100,
            "order_date": datetime.strptime("2023-01-01", "%Y-%m-%d"),
        },
        {
            "order_id": 2,
            "cust_id": 2,
            "order_value": 150,
            "order_date": datetime.strptime("2023-01-02", "%Y-%m-%d"),
        },
    ]
    products = [
        {
            "product_id": 1,
            "product_name": "ProductA",
            "price": 10,
            "created_at": datetime.strptime("2023-01-01", "%Y-%m-%d"),
        },
        {
            "product_id": 2,
            "product_name": "ProductB",
            "price": 20,
            "created_at": datetime.strptime("2023-01-02", "%Y-%m-%d"),
        },
    ]

    schema_customers = (
        "cust_id INT, cust_name STRING, cust_email STRING, created_at DATE"
    )
    schema_orders = "order_id INT, cust_id INT, order_value FLOAT, order_date DATE"
    schema_products = (
        "product_id INT, product_name STRING, price FLOAT, created_at DATE"
    )

    df_customers = spark.createDataFrame(customers, schema=schema_customers)
    df_orders = spark.createDataFrame(orders, schema=schema_orders)
    df_products = spark.createDataFrame(products, schema=schema_products)

    # Write the DataFrames to Delta tables in Databricks.
    df_customers.write.format("parquet").mode("overwrite").save(
        "/Volumes/main/default/raw/customers/"
    )
    df_orders.write.format("parquet").mode("overwrite").save(
        "/Volumes/main/default/raw/orders/"
    )
    df_products.write.format("parquet").mode("overwrite").save(
        "/Volumes/main/default/raw/products/"
    )
