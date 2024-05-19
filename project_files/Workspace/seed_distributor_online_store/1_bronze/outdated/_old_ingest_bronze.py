# Databricks notebook source
from delta.tables import *
from pyspark.sql import functions as F


def file_exists(dir):
    try:
        dbutils.fs.ls(dir)
    except:
        return False  
    return True


source_path = "abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/adv-dse"
working_path = "abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_B/zhastay_yeltay"
bronze = f"{working_path}/bronze"

sa_name = "dextestwesteurope"
sas_token = "sp=racwdlmeop&st=2024-03-04T10:56:06Z&se=2025-01-23T18:56:06Z&sv=2022-11-02&sr=c&sig=e%2BPezBaetl0D7ReapS4f94vCkvTvk%2BJLQz9CMpHiqjA%3D"

spark.conf.set(f"fs.azure.account.auth.type.{sa_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{sa_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{sa_name}.dfs.core.windows.net", sas_token)

dbutils.fs.ls(f'{source_path}')

# COMMAND ----------

# dbutils.fs.mkdirs(f"{bronze}")

dbutils.fs.ls('abfss://dex-data@dextestwesteurope.dfs.core.windows.net/data/Team_B/zhastay_yeltay/bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Addresses

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f"{bronze}/addresses/"):
    addresses_df_upd = spark.read.parquet(f"{source_path}/addresses/")
    addresses_df_upd.write.format('delta').save(f"{bronze}/addresses/")
    print('upd', addresses_df_upd.count())
else:
    addresses_df_upd = spark.read.parquet(f"{source_path}/addresses/")
    addresses_df_raw = DeltaTable.forPath(spark, f"{bronze}/addresses/")

    # print('raw', addresses_df_raw.toDF().count())
    # print('upd', addresses_df_upd.count())

    addresses_df_raw.alias('addresses') \
    .merge(
        addresses_df_upd.alias('updates'),
        'addresses.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
        {
            "createdOn": "updates.createdOn",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "id": "updates.id",
            "addressline": "updates.addressline"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "createdOn": "updates.createdOn",
            "city": "updates.city",
            "state": "updates.state",
            "country": "updates.country",
            "id": "updates.id",
            "addressline": "updates.addressline"
        }
    ) \
    .execute()
    # print('raw', addresses_df_raw.toDF().count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Answers
# MAGIC
# MAGIC if not DeltaTable.isDeltaTable(spark, f"{bronze}/answers/"):
# MAGIC     answers_df_upd = spark.read.parquet(f"{source_path}/answers/")
# MAGIC     answers_df_upd.write.format('delta').save(f"{bronze}/answers/")
# MAGIC     print('upd', answers_df_upd.count())
# MAGIC else:
# MAGIC     answers_df_upd = spark.read.parquet(f"{source_path}/answers/")
# MAGIC     answers_df_raw = DeltaTable.forPath(spark, f"{bronze}/answers/")
# MAGIC
# MAGIC     # print('raw', answers_df_raw.toDF().count())
# MAGIC     # print('upd', answers_df_upd.count())
# MAGIC
# MAGIC     answers_df_raw.alias('answers') \
# MAGIC     .merge(
# MAGIC         answers_df_upd.alias('updates'),
# MAGIC         'answers.id = updates.id AND answers.primaryId = updates.primaryId'
# MAGIC     ) \
# MAGIC     .whenMatchedUpdate(set =
# MAGIC         {
# MAGIC             "id": "updates.id",
# MAGIC             "primaryId": "updates.primaryId"
# MAGIC         }
# MAGIC     ) \
# MAGIC     .whenNotMatchedInsert(values =
# MAGIC         {
# MAGIC             "id": "updates.id",
# MAGIC             "primaryId": "updates.primaryId"
# MAGIC         }
# MAGIC     ) \
# MAGIC     .execute()
# MAGIC     # print('raw', answers_df_raw.toDF().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f"{bronze}/customers/"):
    customers_df_upd = spark.read.parquet(f"{source_path}/customers/")
    customers_df_upd.write.format('delta').save(f"{bronze}/customers/")
    print('upd', customers_df_upd.count())
else:
    customers_df_upd = spark.read.parquet(f"{source_path}/customers/")
    customers_df_raw = DeltaTable.forPath(spark, f"{bronze}/customers/")

    # print('raw', customers_df_raw.toDF().count())
    # print('upd', customers_df_upd.count())

    customers_df_raw.alias('customers') \
    .merge(
        customers_df_upd.alias('updates'),
        'customers.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
        {
            "id": "updates.id",
            "type": "updates.type",
            "status": "updates.status",
            "CreatedOn": "updates.CreatedOn"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "id": "updates.id",
            "type": "updates.type",
            "status": "updates.status",
            "CreatedOn": "updates.CreatedOn"
        }
    ) \
    .execute()
    # print('raw', customers_df_raw.toDF().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Items

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f"{bronze}/items/"):
    items_df_upd = spark.read.parquet(f"{source_path}/items/")
    items_df_upd.write.format('delta').save(f"{bronze}/items/")
    print('upd', items_df_upd.count())
else:
    items_df_upd = spark.read.parquet(f"{source_path}/items/")
    items_df_raw = DeltaTable.forPath(spark, f"{bronze}/items/")

    # print('raw', items_df_raw.toDF().count())
    # print('upd', items_df_upd.count())

    items_df_raw.alias('items') \
    .merge(
        items_df_upd.alias('updates'),
        'items.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
        {
            "Codes": "updates.Codes",
            "Descriptions": "updates.Descriptions",
            "id": "updates.id",
            "price": "updates.price"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "Codes": "updates.Codes",
            "Descriptions": "updates.Descriptions",
            "id": "updates.id",
            "price": "updates.price"
        }
    ) \
    .execute()
    # print('raw', items_df_raw.toDF().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## orderDetails

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f"{bronze}/orderDetails/"):
    orderDetails_df_upd = spark.read.parquet(f"{source_path}/orderDetails/")
    orderDetails_df_upd.write.format('delta').save(f"{bronze}/orderDetails/")
    print('upd', orderDetails_df_upd.count())
else:
    orderDetails_df_upd = spark.read.parquet(f"{source_path}/orderDetails/")
    orderDetails_df_raw = DeltaTable.forPath(spark, f"{bronze}/orderDetails/")

    # print('raw', orderDetails_df_raw.toDF().count())
    # print('upd', orderDetails_df_upd.count())

    orderDetails_df_raw.alias('orderDetails') \
    .merge(
        orderDetails_df_upd.alias('updates'),
        '''
        orderDetails.OrderId = updates.OrderId 
        AND orderDetails.ItemId = updates.ItemId 
        AND orderDetails.Quantity = updates.Quantity
        '''
    ) \
    .whenMatchedUpdate(set =
        {
            "OrderId": "updates.OrderId",
            "ItemId": "updates.ItemId",
            "Quantity": "updates.Quantity"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "OrderId": "updates.OrderId",
            "ItemId": "updates.ItemId",
            "Quantity": "updates.Quantity"
        }
    ) \
    .execute()
    # print('raw', orderDetails_df_raw.toDF().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f"{bronze}/orders/"):
    orders_df_upd = spark.read.parquet(f"{source_path}/orders/")
    orders_df_upd.write.format('delta').save(f"{bronze}/orders/")
    print('upd', orders_df_upd.count())
else:
    orders_df_upd = spark.read.parquet(f"{source_path}/orders/")
    orders_df_raw = DeltaTable.forPath(spark, f"{bronze}/orders/")

    # print('raw', orders_df_raw.toDF().count())
    # print('upd', orders_df_upd.count())

    orders_df_raw.alias('orders') \
    .merge(
        orders_df_upd.alias('updates'),
        'orders.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
        {
            "customerId": "updates.customerId",
            "createdOn": "updates.createdOn",
            "addressId": "updates.addressId",
            "deliveryDate": "updates.deliveryDate",
            "deliveredOn": "updates.deliveredOn",
            "id": "updates.id"
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "customerId": "updates.customerId",
            "createdOn": "updates.createdOn",
            "addressId": "updates.addressId",
            "deliveryDate": "updates.deliveryDate",
            "deliveredOn": "updates.deliveredOn",
            "id": "updates.id"
        }
    ) \
    .execute()
    # print('raw', orders_df_raw.toDF().count())

# COMMAND ----------

