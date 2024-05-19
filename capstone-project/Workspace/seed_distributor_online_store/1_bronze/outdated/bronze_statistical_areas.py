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
# MAGIC ## Statistical Areas

# COMMAND ----------

statistical_areas = pd.read_excel('https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2023/delineation-files/list2_2023.xlsx', skiprows=2, skipfooter=3)
statistical_areas.columns = ['metropolitan_code', 'metropolitan', 'statistical_category', 'city', 'state_code', 'place_code']

statistical_areas_df = spark.createDataFrame(statistical_areas)
statistical_areas_df.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_bronze_name}.statistical_areas')

# COMMAND ----------

