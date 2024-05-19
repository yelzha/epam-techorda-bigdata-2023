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

# MAGIC %md
# MAGIC ## 1. Define TOP-5 cities with the largest number of VIP customers and specify the number of such customers for each of the TOP-5 cities. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 01_cities_by_vip_customer_count.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.01_cities_by_vip_customer_count
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC WITH cities_by_vip AS (
# MAGIC   SELECT
# MAGIC     a.city,
# MAGIC     COUNT(DISTINCT c.id) AS total_vip_customers
# MAGIC   FROM
# MAGIC     hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o ON a.id = o.address_id
# MAGIC     INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c ON o.customer_id = c.id
# MAGIC   WHERE
# MAGIC     c.status = 'VIP'
# MAGIC   GROUP BY
# MAGIC     a.city
# MAGIC )
# MAGIC SELECT
# MAGIC   city,
# MAGIC   total_vip_customers
# MAGIC FROM
# MAGIC   cities_by_vip
# MAGIC ORDER BY
# MAGIC   total_vip_customers DESC;

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 01_cities_by_vip_customer_count has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Determine whether there were orders and their total number at the address: any combination of street and house number. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 02_total_orders_by_address.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.02_total_orders_by_address
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC WITH total_orders_by_address AS (
# MAGIC   SELECT
# MAGIC     a.addressline,
# MAGIC     COUNT(DISTINCT o.id) AS total_orders
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON a.id = o.address_id
# MAGIC   GROUP BY
# MAGIC     a.addressline
# MAGIC   ORDER BY
# MAGIC     total_orders DESC
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM total_orders_by_address;

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 02_total_orders_by_address has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get the total number of customers in the system as well as their number breakdown by customers’ kinds and types.

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 03_total_customers_by_type_status.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.03_total_customers_by_type_status
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC WITH total_customer AS (
# MAGIC   SELECT
# MAGIC     COALESCE(c.type, 'Total') AS type,
# MAGIC     COUNT(id) FILTER(WHERE c.status = 'Regular') AS Regulars,
# MAGIC     COUNT(id) FILTER(WHERE c.status = 'VIP') AS Vips,
# MAGIC     COUNT(id) AS Total
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   GROUP BY
# MAGIC     c.type
# MAGIC   WITH ROLLUP
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM total_customer
# MAGIC ORDER BY type;

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 03_total_customers_by_type_status has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Get a list of all customers in the system breakdown by their kinds and types.

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 04_customer_kinds_types_list.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.04_customer_kinds_types_list
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC SELECT
# MAGIC   type,
# MAGIC   status,
# MAGIC   array_agg(id) AS ids
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.customers
# MAGIC GROUP BY
# MAGIC   type,
# MAGIC   status

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 04_customer_kinds_types_list has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Get a list of affiliate customers who made less than 5 orders in a week. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 05_affiliate_by_weekly_orders.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.05_affiliate_by_weekly_orders
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC WITH affiliate_by_weekly_orders AS (
# MAGIC   SELECT
# MAGIC     c.id AS customer_id,
# MAGIC     c.status AS customer_status,
# MAGIC     (CAST(o.created_on AS DATE) - (dayofweek(o.created_on) + 5) % 7) AS monday_of_week,
# MAGIC     COUNT(*) AS weekly_total_orders
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON a.id = o.address_id
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC     ON o.customer_id = c.id
# MAGIC   WHERE
# MAGIC     c.type = 'Affiliate'
# MAGIC   GROUP BY
# MAGIC     c.id,
# MAGIC     c.status,
# MAGIC     monday_of_week
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_status
# MAGIC FROM affiliate_by_weekly_orders
# MAGIC GROUP BY
# MAGIC   customer_id,
# MAGIC   customer_status
# MAGIC HAVING
# MAGIC   COUNT(*) FILTER(WHERE weekly_total_orders >= 5) = 0

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 05_affiliate_by_weekly_orders has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Get a list of VIP customers and the amount that was paid to each of them as a coupon discount. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 06_total_vip_customers_coupon.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.06_total_vip_customers_coupon
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC SELECT
# MAGIC   c.id AS customer_id,
# MAGIC   5 * COUNT(DISTINCT o.id) AS total_coupon_amount
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON a.id = o.address_id
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   ON o.customer_id = c.id
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.metropolitan_cities AS mc
# MAGIC   ON a.city = mc.city
# MAGIC     AND a.state = mc.state
# MAGIC WHERE
# MAGIC   c.status = 'VIP'
# MAGIC GROUP BY
# MAGIC   c.id
# MAGIC ORDER BY
# MAGIC   total_coupon_amount DESC

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 06_total_vip_customers_coupon has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Find out the average delivery time to a specific city and the average number of orders to specified cities.

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 07_average_time_and_order_count_by_city.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.07_average_time_and_order_count_by_city
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH address_orders AS (
# MAGIC     SELECT
# MAGIC         a.state,
# MAGIC         a.city,
# MAGIC         DATE(o.created_on) AS created_date,
# MAGIC         SUM(DATE_DIFF(o.delivered_on, o.created_on)) AS sum_date_diff,
# MAGIC         COUNT(o.id) AS total_orders
# MAGIC     FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC         ON a.id = o.address_id
# MAGIC     GROUP BY
# MAGIC         a.state,
# MAGIC         a.city,
# MAGIC         created_date
# MAGIC )
# MAGIC SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   ROUND(SUM(sum_date_diff) / SUM(total_orders), 2) AS avg_delivery_day,
# MAGIC   ROUND(AVG(total_orders), 2) AS avg_orders_per_day
# MAGIC FROM address_orders
# MAGIC GROUP BY
# MAGIC   state,
# MAGIC   city

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 07_average_time_and_order_count_by_city has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Get the overall number and share of orders, delivery time of which exceeds 7 days as well as broken down by the order type. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 08_order_delays_by_order_type.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.08_order_delays_by_order_type
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   CASE WHEN mc.metropolitan IS NOT NULL THEN 'Metropolitan' ELSE 'Regional' END AS order_type,
# MAGIC   COUNT(*) FILTER(WHERE DATE_DIFF(o.delivered_on, o.created_on) > 7) AS total_delayed_orders,
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   ROUND(100 * (COUNT(*) FILTER(WHERE DATE_DIFF(o.delivered_on, o.created_on) > 7))::NUMERIC / COUNT(*), 2) AS delayed_percentage
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     ON o.address_id = a.id
# MAGIC LEFT JOIN hive_metastore.zhastay_yeltay_02_silver.metropolitan_cities as mc
# MAGIC     ON mc.state = a.state
# MAGIC     AND mc.city = a.city
# MAGIC GROUP BY
# MAGIC     order_type
# MAGIC ORDER BY
# MAGIC     delayed_percentage DESC

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 08_order_delays_by_order_type has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. For each state-capital city pair get the total number of orders and the number of orders for the whole state.

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 09_total_orders_by_state_capital.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.09_total_orders_by_state_capital
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH city_total_orders AS (
# MAGIC   SELECT
# MAGIC     a.state,
# MAGIC     a.city,
# MAGIC     COUNT(o.id) AS total_orders,
# MAGIC     SUM(COUNT(o.id)) OVER(PARTITION BY a.state) AS total_orders_by_state
# MAGIC   FROM
# MAGIC     hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o ON a.id = o.address_id
# MAGIC   GROUP BY
# MAGIC     a.state,
# MAGIC     a.city
# MAGIC )
# MAGIC SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   total_orders,
# MAGIC   total_orders_by_state
# MAGIC FROM
# MAGIC   city_total_orders
# MAGIC WHERE
# MAGIC   (state, city) IN (
# MAGIC     SELECT
# MAGIC       state,
# MAGIC       city
# MAGIC     FROM
# MAGIC       hive_metastore.zhastay_yeltay_02_silver.state_capital_cities
# MAGIC   )

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 09_total_orders_by_state_capital has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Get complete information on the order and the customer by the given order number. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 10_complete_order_customer_by_id.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.10_complete_order_customer_by_id
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   o.id AS order_id,
# MAGIC   o.created_on AS order_created_on,
# MAGIC   i.descriptions AS item_description,
# MAGIC   i.price AS item_price,
# MAGIC   od.quantity AS item_quantity,
# MAGIC   i.price * od.quantity AS total_item_price,
# MAGIC   SUM(i.price * od.quantity) OVER() AS total_order_price,
# MAGIC   o.customer_id,
# MAGIC   c.type AS customer_type,
# MAGIC   c.status AS customer_status,
# MAGIC   a.addressline AS delivery_addressline,
# MAGIC   a.city AS delivery_city,
# MAGIC   a.state AS delivery_state,
# MAGIC   a.country AS delivery_country,
# MAGIC   mc.metropolitan IS NOT NULL AS is_city_metropolitan,
# MAGIC   scc.state IS NOT NULL AS is_city_state_capital
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.order_details AS od
# MAGIC   ON od.order_id = o.id
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.items AS i
# MAGIC   ON i.id = od.item_id
# MAGIC
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   ON o.customer_id = c.id
# MAGIC
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   ON a.id = o.address_id
# MAGIC LEFT JOIN hive_metastore.zhastay_yeltay_02_silver.metropolitan_cities AS mc
# MAGIC   ON a.state = mc.state
# MAGIC   AND a.city = mc.city
# MAGIC LEFT JOIN hive_metastore.zhastay_yeltay_02_silver.state_capital_cities AS scc
# MAGIC   ON a.state = scc.state
# MAGIC   AND a.city = scc.city
# MAGIC WHERE
# MAGIC   o.id = '2987'

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 10_complete_order_customer_by_id has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Get a list of orders for each of the states for one month. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 11_monthly_orders_by_state.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.11_monthly_orders_by_state
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     a.state,
# MAGIC     EXTRACT(YEAR FROM o.created_on) AS order_year,
# MAGIC     EXTRACT(MONTH FROM o.created_on) AS order_month,
# MAGIC     -- COUNT(o.id) AS total_orders,
# MAGIC     ARRAY_AGG(o.id) AS order_ids
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON a.id = o.address_id
# MAGIC GROUP BY
# MAGIC     a.state,
# MAGIC     order_year,
# MAGIC     order_month
# MAGIC ORDER BY
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     a.state

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 11_monthly_orders_by_state has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Get the number and list of orders made by each affiliate customer. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 12_affiliate_customer_orders.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.12_affiliate_customer_orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     c.id AS customer_id,
# MAGIC     COUNT(o.id) AS total_order
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON o.customer_id = c.id
# MAGIC WHERE
# MAGIC     c.type = 'Affiliate'
# MAGIC GROUP BY
# MAGIC     c.id
# MAGIC ORDER BY
# MAGIC     c.id

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 12_affiliate_customer_orders has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Define the number of orders that were delivered to a specific customer for the entire time and for the certain period. 

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 13_customer_total_order_delivered.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.13_customer_total_order_delivered
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     c.id,
# MAGIC     o.delivered_on,
# MAGIC     COUNT(*) AS daily_total_orders,
# MAGIC     COUNT(*) OVER(PARTITION BY c.id) AS total_orders
# MAGIC     -- COUNT(*) FILTER(WHERE o.delivered_on BETWEEN '2020-01-01' AND '2020-02-01') AS total_orders_by_certain_period
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC     ON o.customer_id = c.id
# MAGIC GROUP BY
# MAGIC     c.id,
# MAGIC     o.delivered_on
# MAGIC ORDER BY
# MAGIC     c.id,
# MAGIC     o.delivered_on

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 13_customer_total_order_delivered has been populated/updated and overwritten successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Get a list and the total number of orders shipped on a specific day in a specific city.

# COMMAND ----------

logger.info("SQL operation started to populate/update the table 14_daily_orders_by_city.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.14_daily_orders_by_city
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     a.city,
# MAGIC     o.delivered_on,
# MAGIC     COUNT(*) AS daily_total_orders
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     ON o.address_id = a.id
# MAGIC GROUP BY
# MAGIC     a.city,
# MAGIC     o.delivered_on
# MAGIC ORDER BY
# MAGIC     a.city,
# MAGIC     o.delivered_on

# COMMAND ----------

logger.info(f"SQL operation completed successfully. Table 14_daily_orders_by_city has been populated/updated and overwritten successfully.")