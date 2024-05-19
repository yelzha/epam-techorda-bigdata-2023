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
# MAGIC ## Metropolitan

# COMMAND ----------

bronze_state_capital_cities_df = spark.read.format("delta").load(f"{bronze}/state_capital_cities")

silver_state_capital_cities_df= (
    bronze_state_capital_cities_df
    .select('city', 'state')
)

silver_state_capital_cities_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_silver_name}.state_capital_cities')

# COMMAND ----------

