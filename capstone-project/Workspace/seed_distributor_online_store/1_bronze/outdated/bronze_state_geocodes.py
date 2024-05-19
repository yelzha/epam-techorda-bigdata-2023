# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import pandas as pd

from init import *
init_spark()

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## State Geocodes

# COMMAND ----------

state_geocodes = pd.read_excel('https://www2.census.gov/programs-surveys/popest/geographies/2022/state-geocodes-v2022.xlsx', skiprows=5)

state_geocodes_df = spark.createDataFrame(state_geocodes) \
    .withColumnsRenamed({
        'Region': 'region',
        'Division': 'division',
        'State (FIPS)': 'state_code',
        'Name': 'state'
    })
state_geocodes_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_bronze_name}.state_geocodes')