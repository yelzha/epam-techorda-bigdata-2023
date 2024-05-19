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

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType
from delta.tables import *

# Assuming logger has been previously configured and imported
logger = logging.getLogger(__name__)

try:
    # Log loading data from the bronze layer
    logger.info(f"Loading orders data from the Delta table at {bronze}/orders.")
    bronze_orders_df = spark.read.format("delta").load(f"{bronze}/orders")

    # Log the start of data transformation and validation
    logger.info("Starting transformation and validation of orders data.")
    silver_orders_df_upd = (
        bronze_orders_df
        .withColumnsRenamed({
            "customerId": "customer_id",
            "createdOn": "created_on",
            "addressId": "address_id",
            "deliveryDate": "delivery_date",
            "deliveredOn": "delivered_on"
        })
        .withColumn(
            "is_valid",
            F.when(
                F.col("customer_id").isNotNull()
                & F.col("created_on").isNotNull()
                & F.col("address_id").isNotNull()
                & F.col("delivery_date").isNotNull()
                & F.col("delivered_on").isNotNull()
                & F.col("id").isNotNull()
                & F.col("created_on").between(
                    F.lit("2000-01-01").cast(TimestampType()), F.current_timestamp()
                )
                & F.col("delivery_date").between(
                    F.lit("2000-01-01").cast(DateType()), F.current_date()
                )
                & F.col("delivered_on").between(
                    F.lit("2000-01-01").cast(DateType()), F.current_date()
                )
                & (F.col("created_on") <= F.col("delivered_on")),
                True,
            ).otherwise(False),
        )
    )

    # Check for invalid entries and log the result
    has_invalid = silver_orders_df_upd.filter(~F.col('is_valid')).limit(1).count() > 0
    logger.info(f"Invalid entries found: {has_invalid}")

    # Log the start of the merge operation into the silver layer
    logger.info("Starting merge operation for orders data into the Silver layer.")
    silver_orders_df = DeltaTable.forPath(spark, f"{silver}/orders/")
    silver_orders_df.alias("orders").merge(
        silver_orders_df_upd
        .filter(F.col('is_valid'))
        .alias("updates"), 
        "orders.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                orders.customer_id != updates.customer_id
                OR orders.created_on != updates.created_on
                OR orders.address_id != updates.address_id
                OR orders.delivery_date != updates.delivery_date
                OR orders.delivered_on != updates.delivered_on
            """,
        set={
            "customer_id": "updates.customer_id",
            "created_on": "updates.created_on",
            "address_id": "updates.address_id",
            "delivery_date": "updates.delivery_date",
            "delivered_on": "updates.delivered_on"
        },
    ).whenNotMatchedInsert(
        values={
            "customer_id": "updates.customer_id",
            "created_on": "updates.created_on",
            "address_id": "updates.address_id",
            "delivery_date": "updates.delivery_date",
            "delivered_on": "updates.delivered_on",
            "id": "updates.id"
        }
    ).execute()

    # Log successful completion of the merge operation
    logger.info("Merge operation completed successfully for orders data.")

except Exception as e:
    logger.error("An error occurred during the processing and merging of orders data.", exc_info=True)
    raise
