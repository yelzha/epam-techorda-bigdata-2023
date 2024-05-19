# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F

from init import *
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_details

# COMMAND ----------

try:
    # Reading updated order details data
    order_details_df_upd = spark.read.parquet(f"{source_path}/orderDetails/")
    logger.info("Updated order details data read successfully from %s.", f"{source_path}/orderDetails/")

    # Accessing the raw Delta table
    order_details_df_raw = DeltaTable.forPath(spark, f"{bronze}/order_details/")
    logger.info("Accessed DeltaTable for raw order details at %s.", f"{bronze}/order_details/")

    # Performing merge operation
    # Note: Only inserting not matched records
    order_details_df_raw.alias("order_details").merge(
        order_details_df_upd.alias("updates"),
        """
            order_details.OrderId = updates.OrderId 
            AND order_details.ItemId = updates.ItemId 
            AND order_details.Quantity = updates.Quantity
        """
    ).whenNotMatchedInsert(
        values={
            "OrderId": "updates.OrderId",
            "ItemId": "updates.ItemId",
            "Quantity": "updates.Quantity",
        }
    ).execute()
    logger.info("Merge operation for new records in order details executed successfully.")

except Exception as e:
    logger.error("An error occurred during the order details data processing: %s", e)
    raise

# COMMAND ----------

