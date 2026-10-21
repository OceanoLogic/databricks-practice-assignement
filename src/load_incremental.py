from datetime import datetime

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from generate_data import generate_example_data
from pyspark.sql.window import Window

spark = DatabricksSession.builder.serverless(True).getOrCreate()

generate_example_data(spark)
LOG_TABLE_PATH = "/Volumes/main/default/raw/_logs/"


def log_etl_status(spark, table_name, status, rows_loaded=0, error_message=None):
    log_data = {
        "table_name": table_name,
        "execution_time": datetime,
        "status": status,
        "rows_loaded": rows_loaded,
        "error_message": error_message,
    }

    log_df = spark.createDataFrame(log_data)

    try:
        if DeltaTable.isDeltaTable(spark, LOG_TABLE_PATH):
            log_df.write.format("delta").mode("append").save(
                LOG_TABLE_PATH + "/" + table_name
            )
        else:
            log_df.write.format("delta").mode("overwrite").save(
                LOG_TABLE_PATH + "/" + table_name
            )
    except Exception as e:
        print(f"Failed to write audit log for table {table_name}")


def load_incremental_data(spark, source_table, target_table):

    source_path = "/Volumes/main/default/raw/"
    target_path = f"main.default.{target_table}"

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option(
                "cloudFiles.schemaLocation",
                f"{source_path}_schemas/{source_table}",
            )
            .load(source_path)
        )
    except Exception as e:
        log_etl_status(spark, source_table, "FAILURE", 0, error_message=str(e))
        raise RuntimeError(f"Failed to initialize stream for {source_table}: {e}")

    df.writeStream.option(
        "checkpointLocation",
        f"{source_path}_checkpoints/{source_table}",
    ).trigger(availableNow=True).toTable(f"main.default.{target_table}", format="delta")


load_incremental_data(spark, "customers", "customers")
