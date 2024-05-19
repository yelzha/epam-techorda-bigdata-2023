# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import pandas as pd
import re

from init import *
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Metropolitan

# COMMAND ----------

try:
    # Log the reading of Excel file into DataFrame
    logger.info(f"Reading statistical areas data from Excel file at /{working_path.replace(':', '')}/{schema_bronze_name}/statistical_areas.xlsx.")
    statistical_areas = pd.read_excel(
        f"/{working_path.replace(':', '')}/{schema_bronze_name}/statistical_areas.xlsx", 
        skiprows=2, 
        skipfooter=3
    )
    statistical_areas.columns = ['metropolitan_code', 'metropolitan', 'statistical_category', 'city', 'state_code', 'place_code']

    # Convert pandas DataFrame to Spark DataFrame
    logger.info("Converting pandas DataFrame to Spark DataFrame.")
    bronze_statistical_areas_df = spark.createDataFrame(statistical_areas)

    # Prepare regex pattern for cleaning city names
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

    # Log the start of data cleaning and filtering
    logger.info("Starting cleaning and filtering of statistical areas data.")
    silver_statistical_areas_df = (
        bronze_statistical_areas_df
        .withColumn("city", F.trim(F.regexp_replace("city", pattern, "")))
        .withColumn("city", F.regexp_replace("city", "Urban Honolulu", "Honolulu"))
        .filter(F.col('statistical_category') == 'Metropolitan Statistical Area')
    )

    # Log successful data preparation
    logger.info("Statistical areas data prepared successfully for further processing.")

except Exception as e:
    logger.error("An error occurred during the processing of statistical areas data.", exc_info=True)
    raise

# COMMAND ----------

try:
    # Log the reading of the state geocodes Excel file
    logger.info(f"Reading state geocodes data from Excel file at /{working_path.replace(':', '')}/{schema_bronze_name}/state_geocodes.xlsx.")
    state_geocodes = pd.read_excel(
        f"/{working_path.replace(':', '')}/{schema_bronze_name}/state_geocodes.xlsx", 
        skiprows=5
    )

    # Convert pandas DataFrame to Spark DataFrame and rename columns
    logger.info("Converting pandas DataFrame to Spark DataFrame and renaming columns.")
    bronze_state_geocodes_df = spark.createDataFrame(state_geocodes) \
        .withColumnsRenamed({
            'Region': 'region',
            'Division': 'division',
            'State (FIPS)': 'state_code',
            'Name': 'state'
        })

    # Log filtering of data to exclude records with state_code 0
    logger.info("Filtering out records where state_code is 0.")
    silver_state_geocodes_df = (
        bronze_state_geocodes_df
        .filter(F.col('state_code') != 0)
        .select('state_code', 'state')
    )

    # Log the completion of the data preparation
    logger.info("State geocodes data prepared successfully for further processing.")

except Exception as e:
    logger.error("An error occurred during the processing of state geocodes data.", exc_info=True)
    raise

# COMMAND ----------


try:
    # Log the start of the data joining process
    logger.info("Starting to join statistical areas data with state geocodes.")
    metropolitan_cities = (
        silver_statistical_areas_df.alias('sa')
        .join(silver_state_geocodes_df.alias('sg'), F.col('sa.state_code') == F.col('sg.state_code'), "inner")
        .select('city', 'state', 'metropolitan')
    )

    # Log the number of records found (if not performance-intensive)
    count = metropolitan_cities.count()
    logger.info(f"Number of metropolitan cities found: {count}")

    count = metropolitan_cities.select('metropolitan').distinct().count()
    logger.info(f"Number of metropolitans found: {count}")

    # Log the start of data saving operation
    logger.info(f"Writing metropolitan cities data to Delta table {catalog_name}.{schema_silver_name}.metropolitan_cities.")
    metropolitan_cities.write.format("delta").mode("overwrite").saveAsTable(f'{catalog_name}.{schema_silver_name}.metropolitan_cities')

    # Log the successful completion of data saving
    logger.info("Metropolitan cities data saved successfully.")

except Exception as e:
    logger.error("An error occurred during the processing and saving of metropolitan cities data.", exc_info=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC # State Capitals

# COMMAND ----------

try:
    # Log loading JSON data
    logger.info(
        f"Loading state capital cities data from JSON at {working_path}/{schema_bronze_name}/state_capital_cities.json."
    )
    bronze_state_capital_cities_df = spark.read.json(
        f"{working_path}/{schema_bronze_name}/state_capital_cities.json"
    )

    state_abbreviations = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "District of Columbia": "DC",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
    }

    states_df = spark.createDataFrame(
        state_abbreviations.items(), ["state_name", "abbreviation"]
    )

    # Log data selection
    logger.info("Selecting city and state columns from the loaded data.")
    silver_state_capital_cities_df = bronze_state_capital_cities_df.select(
        "city", "state"
    )

    # Join the dataframes on the state name
    silver_state_capital_cities_df = silver_state_capital_cities_df.join(
        states_df, silver_state_capital_cities_df.state == states_df.state_name, "left"
    ).select("city", "state", "abbreviation")

    # Log the start of data saving operation
    logger.info(
        f"Writing state capital cities data to Delta table {catalog_name}.{schema_silver_name}.state_capital_cities."
    )
    
    (
        silver_state_capital_cities_df.write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .saveAsTable(f"{catalog_name}.{schema_silver_name}.state_capital_cities")
    )

    # Log successful completion of the data save operation
    logger.info("State capital cities data saved successfully to Delta table.")

except Exception as e:
    logger.error(
        "An error occurred during the processing and saving of state capital cities data.",
        exc_info=True,
    )
    raise

# COMMAND ----------

