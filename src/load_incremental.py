import json
from datetime import datetime

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql.window import Window

from src.generate_data import generate_example_data

spark = DatabricksSession.builder.getOrCreate()

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
    log_data = [
        {
            "table_name": table_name,
            "execution_time": datetime.now(),
            "status": status,
            "rows_loaded": rows_loaded,
            "error_message": error_message,
        }
    ]

    log_df = spark.createDataFrame(log_data)

    try:
        if DeltaTable.isDeltaTable(spark, LOG_TABLE_PATH):
            log_df.write.format("delta").mode("append").save(LOG_TABLE_PATH)
        else:
            log_df.write.format("delta").mode("overwrite").save(LOG_TABLE_PATH)
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

    def upsert_to_delta(micro_batch_df, batch_id):
        """Merge incremental data into existing Delta tables"""

        row_count = micro_batch_df.count()
        if row_count == 0:
            print(f"Micro-batch {batch_id} for {table_name} is empty. Skipping.")
            return

        pk = cfg["pk"]
        target_table = cfg["target_table"]

        try:
            # Deduplicate micro-batch
            dedup_batch_df = micro_batch_df.dropDuplicates([pk])

            # Merge or initial load
            if spark.catalog.tableExists(target_table):
                delta_target = DeltaTable.forName(spark, target_table)
                delta_target.alias("target").merge(
                    dedup_batch_df.alias("source"), f"target.{pk} = source.{pk}"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                dedup_batch_df.write.format("delta").mode("overwrite").saveAsTable(
                    target_table
                )

            # Validation (Row counts & Duplicates)
            final_df = spark.read.table(target_table)
            duplicate_count = (
                final_df.groupBy(pk).count().filter(F.col("count") > 1).count()
            )

            if duplicate_count > 0:
                raise ValueError(
                    f"Validation Failed: Found {duplicate_count} duplicate primary keys"
                    f" ({pk}) in target table {target_table}!"
                )

            print(
                f"Validation Passed for {table_name}. Total rows in target:"
                f" {final_df.count()}"
            )

            # Log Success
            log_etl_status(
                spark, table_name, "SUCCESS", rows_loaded=dedup_batch_df.count()
            )

        except Exception as batch_error:
            print(f"Error processing micro-batch for {table_name}: {batch_error}")
            log_etl_status(spark, table_name, "FAILURE", error_message=str(batch_error))
            raise batch_error

    query = (
        df.writeStream.foreachBatch(upsert_to_delta)
        .option("checkpointLocation", cfg["checkpoint_path"])
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print(f"--- Completed Pipeline for: {table_name} ---")


for name, cfg in config.items():
    load_incremental_data(spark, name, cfg)
