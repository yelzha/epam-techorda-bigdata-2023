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
# MAGIC ## Addresses

# COMMAND ----------

try:
    # Initialize Spark session
    logger.info("Initializing Spark session.")

    # Read addresses data
    logger.info("Reading updated addresses data from %s.", f"{source_path}/addresses/")
    addresses_df_upd = spark.read.parquet(f"{source_path}/addresses/")

    # Accessing Delta table
    logger.info("Accessing Delta table for addresses at %s.", f"{bronze}/addresses/")
    addresses_df_raw = DeltaTable.forPath(spark, f"{bronze}/addresses/")
    
    # Log beginning of merge process
    logger.info("Starting merge operation for addresses.")
    addresses_df_raw.alias("addresses").merge(
        addresses_df_upd.alias("updates"), "addresses.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                addresses.createdOn != updates.createdOn
                OR addresses.city != updates.city
                OR addresses.state != updates.state
                OR addresses.country != updates.country
                OR addresses.addressline != updates.addressline
            """,
        set={
            "createdOn": "updates.createdOn",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "addressline": "updates.addressline",
        },
    ).whenNotMatchedInsert(
        values={
            "createdOn": "updates.createdOn",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "id": "updates.id",
            "addressline": "updates.addressline",
        }
    ).execute()
    
    # Log successful completion of merge process
    logger.info("Merge operation for addresses completed successfully.")

except Exception as e:
    # Log any exceptions that occur
    logger.error("An error occurred during the merge process for addresses.", exc_info=True)
    raise

# COMMAND ----------

