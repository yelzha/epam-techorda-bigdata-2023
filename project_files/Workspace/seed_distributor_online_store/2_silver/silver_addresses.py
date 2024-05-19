# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, LongType, BooleanType

from init import *
from udfs import * 
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking validity

# COMMAND ----------

try:
    # Log loading data from the bronze layer
    logger.info(f"Loading address data from the Delta table at {bronze}/addresses.")
    bronze_addresses_df = spark.read.format("delta").load(f"{bronze}/addresses")

    # Log start of transformation
    logger.info("Starting transformation and validation of address data.")

    # Renaming column and adding validation columns
    silver_addresses_df_upd = (
        bronze_addresses_df
        .withColumnRenamed("CreatedOn", "created_on")
        .withColumn(
            'is_valid',
            F.col('addressline').rlike(address_pattern)  # Valid pattern
            & ~F.col('addressline').rlike(r'^\s*\d+(\s+\d+)*\s*$')  # Checking if there are only numbers
            & F.col('city').rlike(city_state_pattern)
            & F.col('state').rlike(city_state_pattern)
            & F.col('country').rlike(city_state_pattern)
        )
    )

    # Filtering valid and invalid addresses
    silver_addresses_valid = silver_addresses_df_upd.filter(F.col('is_valid'))
    silver_addresses_invalid = silver_addresses_df_upd.exceptAll(silver_addresses_valid)

    # Log completion of filtering
    logger.info("Completed filtering of valid and invalid addresses.")

except Exception as e:
    logger.error("An error occurred during the processing of addresses.", exc_info=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valid

# COMMAND ----------

try:
    # Log loading and preparing for merge
    logger.info(f"Loading Silver layer Delta table for addresses from {silver}/addresses.")
    silver_addresses_df = DeltaTable.forPath(spark, f"{silver}/addresses/")

    # Log the start of the merge operation
    logger.info("Starting merge operation for validated addresses.")
    silver_addresses_df.alias("addresses").merge(
        silver_addresses_valid.alias("updates"),
        "addresses.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                addresses.created_on != updates.created_on
                OR addresses.city != updates.city
                OR addresses.state != updates.state
                OR addresses.country != updates.country
                OR addresses.addressline != updates.addressline
            """,
        set={
            "created_on": "updates.created_on",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "addressline": "updates.addressline"
        },
    ).whenNotMatchedInsert(
        values={
            "created_on": "updates.created_on",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "id": "updates.id",
            "addressline": "updates.addressline"
        }
    ).execute()
    logger.info("Merge operation completed successfully.")
except Exception as e:
    logger.error("An error occurred during the merge or maintenance operations.", exc_info=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLQ

# COMMAND ----------

try:
    # Check if there are invalid addresses
    logger.info("Checking for invalid addresses.")
    has_invalid = silver_addresses_invalid.limit(1).count() > 0

    if has_invalid:
        logger.info("Invalid addresses found, processing them into DLQ (Dead Letter Queue).")

        # Load the DLQ table
        logger.info(f"Loading DLQ Delta table for addresses from {silver}/addresses_dlq.")
        silver_addresses_dlq_df = DeltaTable.forPath(spark, f"{silver}/addresses_dlq/")

        # Log the start of the merge operation into DLQ
        logger.info("Starting merge operation for invalid addresses into DLQ.")
        silver_addresses_dlq_df.alias("addresses").merge(
            silver_addresses_invalid.alias("updates"),
            "addresses.id = updates.id",
        ).whenMatchedUpdate(
            condition="""
                    addresses.created_on != updates.created_on
                    OR addresses.city != updates.city
                    OR addresses.state != updates.state
                    OR addresses.country != updates.country
                    OR addresses.addressline != updates.addressline
                """,
            set={
                "created_on": "updates.created_on",
                "city": "updates.city",
                "state": "updates.state",
                "country": "updates.country",
                "addressline": "updates.addressline",
                "is_processed": "False",
                "dlq_timestamp": F.current_timestamp()
            },
        ).whenNotMatchedInsert(
            values={
                "created_on": "updates.created_on",
                "city": "updates.city",
                "state": "updates.state",
                "country": "updates.country",
                "id": "updates.id",
                "addressline": "updates.addressline",
                "is_processed": "False",
                "dlq_timestamp": F.current_timestamp()
            }
        ).execute()
        logger.info("Merge operation for invalid addresses completed successfully.")
    else:
        logger.info("No invalid addresses found. No DLQ processing required.")

except Exception as e:
    logger.error("An error occurred while processing invalid addresses for DLQ.", exc_info=True)
    raise

# COMMAND ----------

