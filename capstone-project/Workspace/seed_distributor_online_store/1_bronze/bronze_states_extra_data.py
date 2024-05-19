# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F

from init import *
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

logger.info("Starting data download and storage operations.")

# COMMAND ----------

# Download and copy state geocodes
state_geocodes_url = "https://www2.census.gov/programs-surveys/popest/geographies/2022/state-geocodes-v2022.xlsx"
destination_path = f"{working_path}/{schema_bronze_name}/state_geocodes.xlsx"
try:
    dbutils.fs.cp(state_geocodes_url, destination_path)
    logger.info(f"Successfully copied state geocodes from {state_geocodes_url} to {destination_path}.")
except Exception as e:
    logger.error(f"Failed to copy state geocodes. Error: {e}")

# COMMAND ----------

# Download and copy statistical areas data
statistical_areas_url = "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2023/delineation-files/list2_2023.xlsx"
destination_path = f"{working_path}/{schema_bronze_name}/statistical_areas.xlsx"
try:
    dbutils.fs.cp(statistical_areas_url, destination_path)
    logger.info(f"Successfully copied statistical areas data from {statistical_areas_url} to {destination_path}.")
except Exception as e:
    logger.error(f"Failed to copy statistical areas data. Error: {e}")

# COMMAND ----------

# Download and copy state capitals data
state_capital_cities_url = "https://cdn.jsdelivr.net/npm/vega-datasets@v1.29.0/data/us-state-capitals.json"
destination_path = f"{working_path}/{schema_bronze_name}/state_capital_cities.json"
try:
    dbutils.fs.cp(state_capital_cities_url, destination_path)
    logger.info(f"Successfully copied state capitals data from {state_capital_cities_url} to {destination_path}.")
except Exception as e:
    logger.error(f"Failed to copy state capitals data. Error: {e}")

# COMMAND ----------

# Log the completion of all operations
logger.info("Completed all data download and storage operations.")