# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import requests

from init import *
init_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## State Capitals

# COMMAND ----------

url_source = "https://cdn.jsdelivr.net/npm/vega-datasets@v1.29.0/data/us-state-capitals.json"
response = requests.get(url_source)
rdd = spark.sparkContext.parallelize([response.text])
state_capitals_df = spark.read.json(rdd)

state_capitals_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_bronze_name}.state_capital_cities')