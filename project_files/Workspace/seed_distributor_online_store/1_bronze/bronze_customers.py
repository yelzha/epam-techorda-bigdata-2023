# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F

from init import source_path, working_path, bronze, silver, gold
from init import init_spark
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

try:
    # Reading updated customers data
    customers_df_upd = spark.read.parquet(f"{source_path}/customers/")
    logger.info(
        "Updated customers data read successfully from %s.", f"{source_path}/customers/"
    )

    # Accessing the raw Delta table
    customers_df_raw = DeltaTable.forPath(spark, f"{bronze}/customers/")
    logger.info(
        "Accessed DeltaTable for raw customers data at %s.", f"{bronze}/customers/"
    )

    # Performing merge operation
    customers_df_raw.alias("customers").merge(
        customers_df_upd.alias("updates"), "customers.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                customers.type != updates.type
                OR customers.status != updates.status
                OR customers.CreatedOn != updates.CreatedOn""",
        set={
            "type": "updates.type",
            "status": "updates.status",
            "CreatedOn": "updates.CreatedOn",
        },
    ).whenNotMatchedInsert(
        values={
            "id": "updates.id",
            "type": "updates.type",
            "status": "updates.status",
            "CreatedOn": "updates.CreatedOn",
        }
    ).execute()
    logger.info("Merge operation executed successfully.")

except Exception as e:
    logger.error("An error occurred: %s", e)
    raise

# COMMAND ----------

