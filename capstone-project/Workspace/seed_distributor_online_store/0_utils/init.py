# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *

from init import *
init_spark()

# COMMAND ----------

print('Bronze layer is deleted:', dbutils.fs.rm(f"{bronze}", recurse=True))
print('Silver layer is deleted:', dbutils.fs.rm(f"{silver}", recurse=True))
print('Gold layer is deleted:', dbutils.fs.rm(f"{gold}", recurse=True))

print('Working path is deleted:', dbutils.fs.rm(f"{working_path}", recurse=True))

# print('Bronze layer is created:', dbutils.fs.mkdirs(f'{bronze}'))
# print('Silver layer is created:', dbutils.fs.mkdirs(f'{silver}'))
# print('Gold layer is created:', dbutils.fs.mkdirs(f'{gold}'))

# COMMAND ----------

dbutils.fs.rm(working_path, recurse=True)

spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_bronze_name} CASCADE")
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_silver_name} CASCADE")
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_gold_name} CASCADE")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_bronze_name} MANAGED LOCATION '{bronze}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_silver_name} MANAGED LOCATION '{silver}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_gold_name} MANAGED LOCATION '{bronze}'")

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

# addresses
bronze_addresses = DeltaTable.createIfNotExists(spark) \
  .addColumn("createdOn", "TIMESTAMP") \
  .addColumn("city", "STRING") \
  .addColumn("state", "STRING") \
  .addColumn("country", "STRING") \
  .addColumn("id", "BIGINT") \
  .addColumn("addressline", "STRING") \
  .location(f"{bronze}/addresses") \
  .tableName(f'{catalog_name}.{schema_bronze_name}.addresses', ) \
  .execute()

# customers
bronze_customers = DeltaTable.createIfNotExists(spark) \
  .addColumn("id", "BIGINT") \
  .addColumn("type", "STRING") \
  .addColumn("status", "STRING") \
  .addColumn("CreatedOn", "TIMESTAMP") \
  .location(f"{bronze}/customers") \
  .tableName(f'{catalog_name}.{schema_bronze_name}.customers') \
  .execute()


# items
bronze_items = DeltaTable.createIfNotExists(spark) \
  .addColumn("Codes", "STRING") \
  .addColumn("Descriptions", "STRING") \
  .addColumn("id", "BIGINT") \
  .addColumn("price", "DECIMAL(10,2)") \
  .location(f"{bronze}/items") \
  .tableName(f'{catalog_name}.{schema_bronze_name}.items') \
  .execute()


# order_details
bronze_order_details = DeltaTable.createIfNotExists(spark) \
  .addColumn("OrderId", "BIGINT") \
  .addColumn("ItemId", "BIGINT") \
  .addColumn("Quantity", "BIGINT") \
  .location(f"{bronze}/order_details") \
  .tableName(f'{catalog_name}.{schema_bronze_name}.order_details') \
  .execute()


# orders
bronze_orders = DeltaTable.createIfNotExists(spark) \
  .addColumn("customerId", "BIGINT") \
  .addColumn("createdOn", "TIMESTAMP") \
  .addColumn("addressId", "BIGINT") \
  .addColumn("deliveryDate", "DATE") \
  .addColumn("deliveredOn", "DATE") \
  .addColumn("id", "BIGINT") \
  .location(f"{bronze}/orders") \
  .tableName(f'{catalog_name}.{schema_bronze_name}.orders') \
  .execute()

# COMMAND ----------

# # state-capitals
# bronze_state_capitals = DeltaTable.createIfNotExists(spark) \
#   .addColumn("city", "STRING") \
#   .addColumn("state", "STRING") \
#   .location(f"{bronze}/state_capital_cities") \
#   .tableName(f'{catalog_name}.{schema_bronze_name}.state_capital_cities') \
#   .execute()

# # state-geocodes
# bronze_state_geocodes = DeltaTable.createIfNotExists(spark) \
#   .addColumn("region", "BIGINT") \
#   .addColumn("division", "BIGINT") \
#   .addColumn("state_code", "BIGINT") \
#   .addColumn("state", "STRING") \
#   .location(f"{bronze}/state_geocodes") \
#   .tableName(f'{catalog_name}.{schema_bronze_name}.state_geocodes') \
#   .execute()

# # statistical_areas
# bronze_statistical_areas = DeltaTable.createIfNotExists(spark) \
#   .addColumn("metropolitan_code", "BIGINT") \
#   .addColumn("metropolitan", "STRING") \
#   .addColumn("statistical_category", "STRING") \
#   .addColumn("city", "STRING") \
#   .addColumn("state_code", "BIGINT") \
#   .addColumn("place_code", "BIGINT") \
#   .location(f"{bronze}/statistical_areas") \
#   .tableName(f'{catalog_name}.{schema_bronze_name}.statistical_areas') \
#   .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# addresses
silver_addresses = DeltaTable.createIfNotExists(spark) \
  .addColumn("created_on", "TIMESTAMP") \
  .addColumn("city", "STRING") \
  .addColumn("state", "STRING") \
  .addColumn("country", "STRING") \
  .addColumn("id", "BIGINT") \
  .addColumn("addressline", "STRING") \
  .location(f"{silver}/addresses") \
  .tableName(f'{catalog_name}.{schema_silver_name}.addresses') \
  .execute()


# customers
silver_customers = DeltaTable.createIfNotExists(spark) \
  .addColumn("id", "BIGINT") \
  .addColumn("type", "STRING") \
  .addColumn("status", "STRING") \
  .addColumn("created_on", "TIMESTAMP") \
  .location(f"{silver}/customers") \
  .tableName(f'{catalog_name}.{schema_silver_name}.customers') \
  .execute()


# items
silver_items = DeltaTable.createIfNotExists(spark) \
  .addColumn("codes", "STRING") \
  .addColumn("descriptions", "STRING") \
  .addColumn("id", "BIGINT") \
  .addColumn("price", "DECIMAL(10,2)") \
  .location(f"{silver}/items") \
  .tableName(f'{catalog_name}.{schema_silver_name}.items') \
  .execute()


# order_details
silver_order_details = DeltaTable.createIfNotExists(spark) \
  .addColumn("order_id", "BIGINT") \
  .addColumn("item_id", "BIGINT") \
  .addColumn("quantity", "BIGINT") \
  .location(f"{silver}/order_details") \
  .tableName(f'{catalog_name}.{schema_silver_name}.order_details') \
  .execute()


# orders
silver_orders = DeltaTable.createIfNotExists(spark) \
  .addColumn("customer_id", "BIGINT") \
  .addColumn("created_on", "TIMESTAMP") \
  .addColumn("address_id", "BIGINT") \
  .addColumn("delivery_date", "DATE") \
  .addColumn("delivered_on", "DATE") \
  .addColumn("id", "BIGINT") \
  .location(f"{silver}/orders") \
  .tableName(f'{catalog_name}.{schema_silver_name}.orders') \
  .execute()

# COMMAND ----------

# addresses_dql
silver_addresses_dlq = DeltaTable.createIfNotExists(spark) \
  .addColumn("created_on", "TIMESTAMP") \
  .addColumn("city", "STRING") \
  .addColumn("state", "STRING") \
  .addColumn("country", "STRING") \
  .addColumn("id", "BIGINT") \
  .addColumn("addressline", "STRING") \
  .addColumn("is_processed", "BOOLEAN") \
  .addColumn("dlq_timestamp", "TIMESTAMP") \
  .location(f"{silver}/addresses_dlq") \
  .tableName(f'{catalog_name}.{schema_silver_name}.addresses_dlq') \
  .execute()

# COMMAND ----------

# silver_addressesline_enriched
silver_addressline_enriched = DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("addressline", "STRING") \
  .addColumn("country", "STRING") \
  .addColumn("state", "STRING") \
  .addColumn("city", "STRING") \
  .addColumn("full_address", "STRING") \
  .addColumn("completed_address", "STRING") \
  .addColumn("lat", "DOUBLE") \
  .addColumn("lng", "DOUBLE") \
  .addColumn("postal_code", "STRING") \
  .addColumn("county", "STRING") \
  .addColumn("neighborhood", "STRING") \
  .location(f"{silver}/addressline_enriched") \
  .tableName(f'{catalog_name}.{schema_silver_name}.addressline_enriched') \
  .execute()

# COMMAND ----------

# state-capitals
silver_state_capital_cities = DeltaTable.createIfNotExists(spark) \
  .addColumn("city", "STRING") \
  .addColumn("state", "STRING") \
  .location(f"{silver}/state_capital_cities") \
  .tableName(f'{catalog_name}.{schema_silver_name}.state_capital_cities') \
  .execute()

# metropolitan_cities
silver_metropolitan_cities = DeltaTable.createIfNotExists(spark) \
  .addColumn("city", "STRING") \
  .addColumn("state", "STRING") \
  .addColumn("metropolitan", "STRING") \
  .location(f"{silver}/metropolitan_cities") \
  .tableName(f'{catalog_name}.{schema_silver_name}.metropolitan_cities') \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

