# Databricks notebook source
!pip install geocoder tqdm

# COMMAND ----------

import requests
import urllib.parse
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


def get_address_info(address):
    try:
        g = geocoder.bing(address, key='Aq_LUq5E6MHRgYGwGSj0GNir7jKS_vZgDfRBqmBjpBNxkN9P5tXbQYA_hruk1XdS')
        data = g.json
        return (
            data['lat'],
            data['lng'],
            data['postal'],
            data['neighborhood'] if 'neighborhood' in data else None,
            data['raw']['address']['adminDistrict2'],
            data['address']
        )
    except Exception as e:
        print(e)
        return (None, None, None, None, None, None)
    
coord_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("postal_code", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("county", StringType(), True),
    StructField("completed_address", StringType(), True)
])

get_address_info_udf = F.udf(get_address_info, coord_schema)

get_address_info('63550 Skagit Way, Marblemount, Washington, US')

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

unique_addresslines = (
    spark.table("hive_metastore.zhastay_yeltay_02_silver.addresses")
    .select("addressline", "country", "state", "city")
    .distinct()
    .withColumn(
        "full_address",
        F.concat(
            F.col("addressline"),
            F.lit(", "),
            F.col("city"),
            F.lit(", "),
            F.col("state"),
            F.lit(", "),
            F.col("country"),
        ),
    )
)

# result = unique_addresslines.withColumn(
#     "id", F.row_number().over(Window.orderBy("addressline"))
# ).select("id", get_address_info_udf(F.col("full_address")))

# display(result)

# COMMAND ----------

address_lines = unique_addresslines.collect()
address_lines

# COMMAND ----------

import pandas as pd

df = pd.DataFrame([i.asDict()['full_address'] for i in address_lines], columns=['full_address'])
df

# COMMAND ----------

import tqdm

results = []
full_addresses = [i.asDict()['full_address'] for i in address_lines]
for addr in tqdm.tqdm(full_addresses):
    results.append(get_address_info(addr))

# COMMAND ----------

df['info'] = results

# COMMAND ----------

get_address_info('63550 Skagit Way, Marblemount, Washington, USA')

# COMMAND ----------

df