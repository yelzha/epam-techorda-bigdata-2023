# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *

from init import *
init_spark()

# COMMAND ----------

# import os

# file_path = '/tmp/zhastay_yeltay_log.log'
# if os.path.exists(file_path):
#     os.remove(file_path)

# COMMAND ----------

import pandas as pd
import re

try:

    # Step 1: Read the entire log file as a single string
    with open('/tmp/zhastay_yeltay_log.log', 'r') as file:
        log_content = file.read()

    # Step 2: Define a regex pattern to identify the start of each log entry
    # Assuming the log starts with a date in the format 'dd/mm/yyyy hh:mm:ss'
    log_entry_pattern = re.compile(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}↭')

    # Step 3: Find all matches and their positions in the file
    matches = list(log_entry_pattern.finditer(log_content))

    # Step 4: Split the content into log entries based on these positions
    logs = []
    start = 0
    for match in matches:
        end = match.start()
        if end != 0:
            logs.append(log_content[start:end])  # Append the log entry from the previous match to the current match
        start = end

    # Append the last log entry
    logs.append(log_content[start:])

    # Step 5: Create a DataFrame from the list of logs
    df_pandas = pd.DataFrame([x.split('↭', 3) for x in logs if x.strip() != ''], columns=['log_date', 'task_name', 'log_level', 'log_message'])

    df_spark = spark.createDataFrame(df_pandas)

    df_spark.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f'{catalog_name}.{schema_silver_name}.logs')

    with open('/tmp/zhastay_yeltay_log.log', 'w') as f:
        f.write('')
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DELETE
# MAGIC -- FROM hive_metastore.zhastay_yeltay_02_silver.logs
# MAGIC -- WHERE job_name IS NULL;
# MAGIC
# MAGIC SELECT * FROM hive_metastore.zhastay_yeltay_02_silver.logs
# MAGIC ORDER BY 
# MAGIC   log_date DESC

# COMMAND ----------

