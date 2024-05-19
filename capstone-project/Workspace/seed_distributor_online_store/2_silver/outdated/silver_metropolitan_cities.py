# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, LongType

import pandas as pd

from init import *
from udfs import * 
init_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metropolitan cities

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

silver_statistical_areas_df = (
    bronze_statistical_areas_df
    .withColumn("city", F.trim(F.regexp_replace("city", pattern, "")))
    .filter(F.col('statistical_category') == 'Metropolitan Statistical Area')
)

bronze_state_geocodes_df = spark.read.format("delta").load(f"{bronze}/state_geocodes")
silver_state_geocodes_df= (
    bronze_state_geocodes_df
    .filter(F.col('state_code')  != 0)
    .select('state_code', 'state')
)

metropolitan_cities = (
    silver_statistical_areas_df.alias('sa')
    .join(silver_state_geocodes_df.alias('sg'), F.col('sa.state_code') == F.col('sg.state_code'), "inner")
    .select('city', 'state', 'metropolitan')
)


metropolitan_cities.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_silver_name}.metropolitan_cities')