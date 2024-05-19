from databricks.sdk.runtime import *
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from init import init_spark

init_spark()

class SparkSqlHandler(logging.Handler):
    def __init__(self, spark):
        super().__init__()
        self.spark = spark
    
    def emit(self, record):
        try:
            self.spark.sql(f"""
                           INSERT INTO hive_metastore.zhastay_yeltay_01_bronze.logs (log_date, log_level, log_message) 
                           VALUES (current_timestamp(), '{record.levelname}', '{self.format(record)}')
                           """)
        except Exception as e:
            print(f"Failed to log to Hive table: {str(e)}")  # Fallback logging

# Set up logging
logger = logging.getLogger("AzureDatabricksLogger")
logger.setLevel(logging.INFO)

# Add custom Spark SQL handler
spark_handler = SparkSqlHandler(spark)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
spark_handler.setFormatter(formatter)
logger.addHandler(spark_handler)

# Example usage
logger.info("This is an informational message.")
logger.error("This is an error message.")