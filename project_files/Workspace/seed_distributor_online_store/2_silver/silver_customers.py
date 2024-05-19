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
    logger.info(f"Loading customer data from the Delta table at {bronze}/customers.")
    bronze_customers_df = spark.read.format("delta").load(f"{bronze}/customers")

    # Log start of data transformation
    logger.info("Starting transformation of customer data.")
    silver_customers_df_upd = (
        bronze_customers_df
        .withColumnRenamed("CreatedOn", "created_on")
        .withColumn("type", capitalize_udf(F.col("type")))
        .withColumn("status", F.when(F.col('status') == 'VIP', F.col('status')).otherwise(capitalize_udf(F.col("status"))))
        .withColumn(
            "is_valid",
            F.when(
                F.col("id").isNotNull()
                & F.col("type").isNotNull()
                & F.col("status").isNotNull()
                & F.col("created_on").isNotNull()
                & (F.col("type").isin(["Affiliate", "Individual"]))
                & (F.col("status").isin(["Regular", "VIP"]))
                & F.col("created_on").between(
                    F.lit("2000-01-01").cast(TimestampType()), F.current_timestamp()
                ),
                True,
            ).otherwise(False),
        )
    )

    has_invalid = silver_customers_df_upd.filter(~F.col('is_valid')).limit(1).count() > 0
    logger.info(f"Invalid entries found: {has_invalid}")

    # Log start of merge operation into the silver layer
    logger.info("Starting merge operation for customer data into the Silver layer.")
    silver_customers_df = DeltaTable.forPath(spark, f"{silver}/customers/")
    silver_customers_df.alias("customers").merge(
        silver_customers_df_upd.alias("updates"), "customers.id = updates.id"
    ).whenMatchedUpdate(
        condition="""
                customers.type != updates.type
                OR customers.status != updates.status
                OR customers.created_on != updates.created_on
            """,
        set={
            "type": "updates.type",
            "status": "updates.status",
            "created_on": "updates.created_on"
        },
    ).whenNotMatchedInsert(
        values={
            "id": "updates.id",
            "type": "updates.type",
            "status": "updates.status",
            "created_on": "updates.created_on"
        }
    ).execute()

    # Log the successful completion of the merge operation
    logger.info("Merge operation completed successfully for customer data.")

except Exception as e:
    logger.error("An error occurred during the processing and merging of customer data.", exc_info=True)
    raise

# COMMAND ----------

