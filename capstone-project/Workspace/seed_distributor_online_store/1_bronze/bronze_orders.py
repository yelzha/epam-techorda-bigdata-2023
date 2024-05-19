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
# MAGIC ## Orders

# COMMAND ----------

try:
    # Reading updated orders data
    orders_df_upd = spark.read.parquet(f"{source_path}/orders/")
    logger.info("Updated orders data read successfully from %s.", f"{source_path}/orders/")

    # Accessing the raw Delta table
    orders_df_raw = DeltaTable.forPath(spark, f"{bronze}/orders/")
    logger.info("Accessed DeltaTable for raw orders at %s.", f"{bronze}/orders/")

    # Performing merge operation
    orders_df_raw.alias("orders").merge(
        orders_df_upd.alias("updates"), "orders.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                orders.customerId != updates.customerId
                OR orders.createdOn != updates.createdOn
                OR orders.addressId != updates.addressId
                OR orders.deliveryDate != updates.deliveryDate
                OR orders.deliveredOn != updates.deliveredOn
            """,
        set={
            "customerId": "updates.customerId",
            "createdOn": "updates.createdOn",
            "addressId": "updates.addressId",
            "deliveryDate": "updates.deliveryDate",
            "deliveredOn": "updates.deliveredOn",
        }
    ).whenNotMatchedInsert(
        values={
            "customerId": "updates.customerId",
            "createdOn": "updates.createdOn",
            "addressId": "updates.addressId",
            "deliveryDate": "updates.deliveryDate",
            "deliveredOn": "updates.deliveredOn",
            "id": "updates.id",
        }
    ).execute()
    logger.info("Merge operation for orders executed successfully.")

except Exception as e:
    logger.error("An error occurred during the orders data processing: %s", e)
    raise

# COMMAND ----------

