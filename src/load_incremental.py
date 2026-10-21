import json
from datetime import datetime

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from generate_data import generate_example_data
from pyspark.sql.window import Window

spark = DatabricksSession.builder.serverless(True).getOrCreate()

generate_example_data(spark)


def load_configuration(config):
    """Load the configuration paths"""
    try:
        with open(config) as f:
            configuration = json.load(f)
    except FileNotFoundError:
        print("Configuration file not found.")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
    return configuration


config = load_configuration("configurations/config.json")
LOG_TABLE_PATH = "/Volumes/main/default/_logs/"


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
        if DeltaTable.isDeltaTable(spark, config):
            log_df.write.format("delta").mode("append").save(
                config["logs"] + table_name
            )
        else:
            log_df.write.format("delta").mode("overwrite").save(
                config["logs"] + table_name
            )
    except Exception as e:
        print(f"Failed to write audit log for table {table_name}")


def load_incremental_data(spark, table_name, cfg):

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", cfg["schema_path"])
            .load(cfg["source_path"])
        )
    except Exception as e:
        log_etl_status(spark, table_name, "FAILURE", 0, error_message=str(e))
        raise RuntimeError(f"Failed to initialize stream for {table_name}: {e}")

    df.writeStream.option("checkpointLocation", cfg["checkpoint_path"]).trigger(
        availableNow=True
    ).toTable(f"main.default.{table_name}", format="delta")


for name, config in config.items():
    load_incremental_data(spark, name, config)
