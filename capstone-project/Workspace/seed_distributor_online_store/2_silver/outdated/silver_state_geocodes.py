# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import pandas as pd

from init import *
init_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## State Geocodes

# COMMAND ----------

bronze_state_geocodes_df = spark.read.format("delta").load(f"{bronze}/state_geocodes")

silver_state_geocodes_df= (
    bronze_state_geocodes_df
    .filter(F.col('state_code')  != 0)
    .select('state_code', 'state')
)

silver_state_geocodes_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_silver_name}.state_geocodes')

# COMMAND ----------

