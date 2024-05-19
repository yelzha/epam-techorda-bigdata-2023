# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, LongType

from init import *
from udfs import * 
init_spark()

# COMMAND ----------

import requests

url = "https://adb-7218202347847239.19.azuredatabricks.net/api/2.1/jobs/list"

run_id = "805041373030390"
job_id = "179671386617459"

headers = {
    "Authorization": "Bearer dapi39e76086a085852a30322e3fb02c6f41-2",
    "Content-Type": "application/json"
}

params = {
    "run_id": run_id,
    "job_id": job_id,
    "limit": 25
}

response = requests.get(url, headers=headers, params=params)
output = response.json()

import pandas as pd

# pd.DataFrame(output['runs'])
output

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW zhastay_kpi_view AS
# MAGIC WITH bronze_histories AS (
# MAGIC   SELECT 
# MAGIC     'bronze_addresses' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_01_bronze.addresses)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'bronze_customers' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_01_bronze.customers)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'bronze_items' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_01_bronze.items)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'bronze_order_details' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_01_bronze.order_details)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'bronze_orders' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_01_bronze.orders)
# MAGIC ), bronze_kpi AS (
# MAGIC   SELECT
# MAGIC     tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     COALESCE(operationMetrics.executionTimeMs, 0) AS executionTimeMs,
# MAGIC     COALESCE(operationMetrics.numTargetRowsInserted, 0) AS numTargetRowsInserted,
# MAGIC     COALESCE(operationMetrics.numTargetRowsUpdated, 0) AS numTargetRowsUpdated,
# MAGIC     COALESCE(operationMetrics.numOutputRows, 0) AS numOutputRows,
# MAGIC     COALESCE(operationMetrics.numSourceRows, 0) AS numSourceRows
# MAGIC   FROM bronze_histories
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM bronze_kpi;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.zhastay_yeltay_02_silver.kpis 
# MAGIC USING DELTA AS
# MAGIC SELECT *
# MAGIC FROM zhastay_kpi_view
# MAGIC WHERE 1=0;
# MAGIC
# MAGIC MERGE INTO hive_metastore.zhastay_yeltay_02_silver.kpis AS tgt
# MAGIC USING zhastay_kpi_view AS src
# MAGIC   ON src.tablename = tgt.tablename
# MAGIC   AND src.version = tgt.version
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     executionTimeMs = src.executionTimeMs,
# MAGIC     numTargetRowsInserted = src.numTargetRowsInserted,
# MAGIC     numTargetRowsUpdated = src.numTargetRowsUpdated,
# MAGIC     numOutputRows = src.numOutputRows,
# MAGIC     numSourceRows = src.numSourceRows
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (tablename, version, timestamp, operation, executionTimeMs, numTargetRowsInserted, numTargetRowsUpdated, numOutputRows, numSourceRows)
# MAGIC   VALUES (src.tablename, src.version, src.timestamp, src.operation, src.executionTimeMs, src.numTargetRowsInserted, src.numTargetRowsUpdated, src.numOutputRows, src.numSourceRows);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH silver_histories AS (
# MAGIC   SELECT 
# MAGIC     'silver_addresses' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.addresses)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_customers' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.customers)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_items' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.items)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_order_details' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.order_details)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_orders' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.orders)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_addresses_dlq' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.addresses_dlq)
# MAGIC   WHERE operation = 'MERGE'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_addressline_enriched' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.addressline_enriched)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_metropolitan_cities' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.metropolitan_cities)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'silver_state_capital_cities' AS tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics
# MAGIC   FROM (DESCRIBE HISTORY hive_metastore.zhastay_yeltay_02_silver.state_capital_cities)
# MAGIC ), silver_kpi AS (
# MAGIC   SELECT
# MAGIC     tablename,
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     COALESCE(operationMetrics.executionTimeMs, 0) AS executionTimeMs,
# MAGIC     COALESCE(operationMetrics.numTargetRowsInserted, 0) AS numTargetRowsInserted,
# MAGIC     COALESCE(operationMetrics.numTargetRowsUpdated, 0) AS numTargetRowsUpdated,
# MAGIC     COALESCE(operationMetrics.numOutputRows, 0) AS numOutputRows,
# MAGIC     COALESCE(operationMetrics.numSourceRows, 0) AS numSourceRows
# MAGIC   FROM silver_histories
# MAGIC )
# MAGIC
# MAGIC MERGE INTO hive_metastore.zhastay_yeltay_02_silver.kpis AS tgt
# MAGIC USING silver_kpi AS src
# MAGIC   ON src.tablename = tgt.tablename
# MAGIC   AND src.version = tgt.version
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     executionTimeMs = src.executionTimeMs,
# MAGIC     numTargetRowsInserted = src.numTargetRowsInserted,
# MAGIC     numTargetRowsUpdated = src.numTargetRowsUpdated,
# MAGIC     numOutputRows = src.numOutputRows,
# MAGIC     numSourceRows = src.numSourceRows
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (tablename, version, timestamp, operation, executionTimeMs, numTargetRowsInserted, numTargetRowsUpdated, numOutputRows, numSourceRows)
# MAGIC   VALUES (src.tablename, src.version, src.timestamp, src.operation, src.executionTimeMs, src.numTargetRowsInserted, src.numTargetRowsUpdated, src.numOutputRows, src.numSourceRows);

# COMMAND ----------

