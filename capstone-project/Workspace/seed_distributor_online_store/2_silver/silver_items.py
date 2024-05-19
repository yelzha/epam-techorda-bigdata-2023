# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, LongType

from init import *
from udfs import * 
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

try:
    # Log the loading of data from the bronze layer
    logger.info(f"Loading item data from the Delta table at {bronze}/items.")
    bronze_items_df = (
        spark.read.format("delta").load(f"{bronze}/items")
    )

    # Log the start of data transformation
    logger.info("Starting transformation and validation of item data.")
    silver_items_df_upd = (
        bronze_items_df
        .withColumnsRenamed({
            "Codes": "codes",
            "Descriptions": "descriptions"
        })
        .withColumn(
            "is_valid",
            F.when(
                F.col("codes").isNotNull()
                & F.col("descriptions").isNotNull()
                & F.col("id").isNotNull()
                & F.col("price").isNotNull()
                & (F.length(F.col('codes')) == 10)
                & (F.length(F.col('descriptions')) > 0)
                & (F.col("price") > 0),
                True,
            ).otherwise(False),
        )
    )

    # Check for invalid items and log the result
    has_invalid = silver_items_df_upd.filter(~F.col('is_valid')).limit(1).count() > 0
    logger.info(f"Invalid entries found: {has_invalid}")

    # Log the start of the merge operation
    logger.info("Starting merge operation for item data into the Silver layer.")
    silver_items_df = DeltaTable.forPath(spark, f"{silver}/items/")
    silver_items_df.alias("items").merge(
        silver_items_df_upd.alias("updates"), "items.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                items.codes != updates.codes
                OR items.descriptions != updates.descriptions
                OR items.price != updates.price
            """,
        set={
            "codes": "updates.codes",
            "descriptions": "updates.descriptions",
            "price": "updates.price"
        }
    ).whenNotMatchedInsert(
        values={
            "codes": "updates.codes",
            "descriptions": "updates.descriptions",
            "id": "updates.id",
            "price": "updates.price"
        }
    ).execute()

    # Log successful completion of the merge operation
    logger.info("Merge operation completed successfully for item data.")

except Exception as e:
    logger.error("An error occurred during the processing and merging of item data.", exc_info=True)
    raise

# COMMAND ----------

