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
# MAGIC ## Items

# COMMAND ----------

try:
    # Reading updated items data
    items_df_upd = spark.read.parquet(f"{source_path}/items/")
    logger.info("Updated items data read successfully from %s.", f"{source_path}/items/")

    # Accessing the raw Delta table
    items_df_raw = DeltaTable.forPath(spark, f"{bronze}/items/")
    logger.info("Accessed DeltaTable for raw items data at %s.", f"{bronze}/items/")

    # Performing merge operation
    items_df_raw.alias("items").merge(
        items_df_upd.alias("updates"), "items.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                items.Codes != updates.Codes
                OR items.Descriptions != updates.Descriptions
                OR items.price != updates.price""",
        set={
            "Codes": "updates.Codes",
            "Descriptions": "updates.Descriptions",
            "price": "updates.price",
        }
    ).whenNotMatchedInsert(
        values={
            "Codes": "updates.Codes",
            "Descriptions": "updates.Descriptions",
            "id": "updates.id",
            "price": "updates.price",
        }
    ).execute()
    logger.info("Merge operation executed successfully for items data.")

except Exception as e:
    logger.error("An error occurred during the items data processing: %s", e)
    raise

# COMMAND ----------

