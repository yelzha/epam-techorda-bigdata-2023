# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import re

from init import *
init_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical Areas

# COMMAND ----------

bronze_statistical_areas_df = spark.read.format("delta").load(
    f"{bronze}/statistical_areas"
)

check_list = [
    '(balance)',
    'government',
    'metropolitan',
    'metro',
    'unified',
    'government',
    'consolidated',
    'County'
]
pattern = '|'.join(re.escape(word) for word in set(check_list))

silver_statistical_areas_df = bronze_statistical_areas_df.withColumn(
    "city", F.trim(F.regexp_replace("city", pattern, ""))
)


silver_statistical_areas_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_silver_name}.statistical_areas"
)

# COMMAND ----------

