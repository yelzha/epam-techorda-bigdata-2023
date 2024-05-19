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

try:
    # Load the DLQ Delta table as DataFrame
    logger.info(f"Loading DLQ Delta table from {silver}/addresses_dlq.")
    dlq_delta = DeltaTable.forPath(spark, f"{silver}/addresses_dlq/")
    silver_addresses_dlq_df = dlq_delta.toDF()

    # Log processing start
    logger.info("Starting to process unprocessed DLQ entries.")

    # Cleaning and validating the addresses
    silver_addresses_df_upd = (
        silver_addresses_dlq_df
        .filter(F.col('is_processed') == False)
        .withColumn("city", clean_text_capitalize_words_udf(F.col("city")))
        .withColumn("state", clean_text_capitalize_words_udf(F.col("state")))
        .withColumn("country", clean_text_capitalize_words_udf(F.col("country")))
        .withColumn("country", F.when(F.col("country") == 'Us', 'US').otherwise(F.col("country")))
        .withColumn("addressline", clean_text_capitalize_words_udf(F.col("addressline")))
        .withColumn(
            'is_valid', 
            F.col('addressline').rlike(address_pattern)
            & ~F.col('addressline').rlike(r'^\s*\d+(\s+\d+)*\s*$')  # Checking if there are only numbers
            & F.col('city').rlike(city_state_pattern)
            & F.col('state').rlike(city_state_pattern)
            & (F.col('country') == "US")
        )
    )

    # Filter out valid and invalid addresses
    valid_dlq = silver_addresses_df_upd.filter(F.col('is_valid'))
    invalid_dlq = silver_addresses_df_upd.exceptAll(valid_dlq)

    # Log the outcome
    logger.info(f"Processed DLQ.")

except Exception as e:
    logger.error("An error occurred while processing DLQ entries.", exc_info=True)
    raise

# COMMAND ----------

try:
    # Load the Silver Delta table for addresses
    logger.info(f"Loading Silver Delta table for addresses from {silver}/addresses.")
    silver_addresses_df = DeltaTable.forPath(spark, f"{silver}/addresses/")

    # Log the start of the merge operation
    logger.info("Starting merge operation for validated DLQ entries into the main Silver addresses table.")
    silver_addresses_df.alias("addresses").merge(
        valid_dlq.alias("updates"), "addresses.id = updates.id"
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

    # Log the successful completion of the merge operation
    logger.info("Merge operation for validated DLQ entries completed successfully.")

except Exception as e:
    logger.error("An error occurred during the merge operation for DLQ entries.", exc_info=True)
    raise

# COMMAND ----------

try:
    # Load the DLQ Delta table
    logger.info("Accessing the DLQ Delta table to update entries.")
    dlq_delta = DeltaTable.forPath(spark, f"{silver}/addresses_dlq/")

    # Log the start of the merge operation to delete valid processed entries
    logger.info("Starting merge operation to delete valid entries from the DLQ and update remaining entries.")
    (
        dlq_delta.alias("dlq").merge(
            valid_dlq.alias("valid"), "dlq.id = valid.id"
        )
        .whenMatchedDelete()
        .whenNotMatchedBySourceUpdate(set={
            "is_processed": "True"
        })
        .execute()
    )

    # Log successful completion of updating DLQ
    logger.info("DLQ has been successfully updated. Valid entries deleted and unprocessed entries marked as processed.")

except Exception as e:
    logger.error("An error occurred during the updating of the DLQ.", exc_info=True)
    raise

# COMMAND ----------

