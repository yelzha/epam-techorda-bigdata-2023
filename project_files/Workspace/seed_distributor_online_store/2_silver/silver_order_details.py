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
    # Log loading data from the bronze layer
    logger.info(f"Loading order details data from the Delta table at {bronze}/order_details.")
    bronze_order_details_df = spark.read.format("delta").load(f"{bronze}/order_details")

    # Log start of data transformation
    logger.info("Starting transformation and validation of order details data.")
    silver_order_details_df_upd = (
        bronze_order_details_df.withColumnsRenamed({
            "OrderId": "order_id",
            "ItemId": "item_id",
            "Quantity": "quantity"
        })
        .withColumn(
            "is_valid",
            F.col("order_id").isNotNull()
            & F.col("item_id").isNotNull()
            & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
        )
    )

    # Check for invalid entries and log the result
    has_invalid = silver_order_details_df_upd.filter(~F.col('is_valid')).limit(1).count() > 0
    logger.info(f"Invalid entries found: {has_invalid}")

    # Filtering and aggregating valid entries
    logger.info("Aggregating valid order details entries.")
    silver_order_details_df_upd = (
        silver_order_details_df_upd
        .groupBy(F.col("order_id"), F.col("item_id"))
        .agg(F.sum(F.col("quantity")).alias("quantity"))
    )

    # Log start of the merge operation into the silver layer
    logger.info("Starting merge operation for order details into the Silver layer.")
    silver_order_details_df = DeltaTable.forPath(spark, f"{silver}/order_details/")
    silver_order_details_df.alias("order_details").merge(
        silver_order_details_df_upd.alias("updates"),
        "order_details.order_id = updates.order_id AND order_details.item_id = updates.item_id"
    ).whenMatchedUpdate(
        condition="order_details.quantity != updates.quantity",
        set={"quantity": "updates.quantity"}
    ).whenNotMatchedInsert(
        values={
            "order_id": "updates.order_id",
            "item_id": "updates.item_id",
            "quantity": "updates.quantity"
        }
    ).execute()

    # Log successful completion of the merge operation
    logger.info("Merge operation completed successfully for order details.")

except Exception as e:
    logger.error("An error occurred during the processing and merging of order details data.", exc_info=True)
    raise