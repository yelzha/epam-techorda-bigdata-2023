# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, LongType

from init import source_path, working_path, bronze, silver, gold
from init import init_spark
from udfs import * 
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

!pip install folium

# COMMAND ----------

import folium
import random


data = (
    spark
    .table("hive_metastore.zhastay_yeltay_03_gold.01v2_cities_by_vip_customer_count")
    .select("lat", "lng", F.col('total_vip_customers').alias("cnt"), "city")
    .orderBy(F.col('cnt').desc())
    .filter(
        (F.col('city') != 'Hamilton')
        & (F.col('cnt') > 250)
    )
    # .limit(50)
    .collect()
)

def random_color():
    return '#{:06x}'.format(random.randint(0, 0xFFFFFF))

map_center_lat = sum(lat for lat, lon, cnt, city in data) / len(data)
map_center_lon = sum(lon for lat, lon, cnt, city in data) / len(data)
mymap = folium.Map(location=[map_center_lat, map_center_lon], zoom_start=7)


for row in data:
    popup_text = f"City: {row['city']}<br> VIP: {row['cnt']}"
    folium.Circle(
        location=[row['lat'], row['lng']],
        radius=(row['cnt'] * 10), 
        color=random_color(),
        fill=True,
        fill_color=random_color(),
        popup=popup_text
    ).add_to(mymap)

display(mymap)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC FROM hive_metastore.zhastay_yeltay_03_gold.01v2_cities_by_vip_customer_count

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC FROM zhastay_yeltay_03_gold.01_cities_by_vip_customer_count
# MAGIC ORDER BY
# MAGIC     total_vip_customers DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   state,
# MAGIC   is_metropolitan,
# MAGIC   sum(coalesce(cnt, 0)) AS cnt,
# MAGIC   sum(coalesce(total_revenue, 0)) AS total_revenue
# MAGIC FROM zhastay_yeltay_03_gold.total_revenue_by_all_categories
# MAGIC GROUP BY
# MAGIC   state,
# MAGIC   is_metropolitan
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC *
# MAGIC FROM zhastay_yeltay_03_gold.`05_affiliate_by_weekly_orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC delivered_on,
# MAGIC revenue AS val
# MAGIC FROM zhastay_yeltay_03_gold.daily_total_revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   COUNT(*) FILTER(WHERE not reached_at_least_5_orders_all_weeks)::NUMERIC AS total_inactive_customers,
# MAGIC   COUNT(*) AS total_customers
# MAGIC FROM zhastay_yeltay_03_gold.customers_havent_reached_at_least_5_orders_all_weeks
# MAGIC GROUP BY
# MAGIC   state,
# MAGIC   city
# MAGIC