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

# COMMAND ----------

# MAGIC %md
# MAGIC # ANNEX 1
# MAGIC
# MAGIC ### 1. Define TOP-5 cities with the largest number of VIP customers and specify the number of such customers for each of the TOP-5 cities. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC WITH top_cities_by_vip AS (
# MAGIC   SELECT
# MAGIC     a.city,
# MAGIC     COUNT(DISTINCT c.id) AS total_vip_customers,
# MAGIC     DENSE_RANK() OVER(ORDER BY COUNT(DISTINCT c.id) DESC) AS rnk
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON a.id = o.address_id
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC     ON o.customer_id = c.id
# MAGIC   WHERE
# MAGIC     c.status = 'VIP'
# MAGIC   GROUP BY
# MAGIC     a.city
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   city,
# MAGIC   total_vip_customers
# MAGIC FROM top_cities_by_vip
# MAGIC WHERE 
# MAGIC   rnk <= 5
# MAGIC ORDER BY
# MAGIC   total_vip_customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Determine whether there were orders and their total number at the address: any combination of street and house number.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC WITH total_orders_by_street AS (
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
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM total_orders_by_street

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Get the total number of customers in the system as well as their number breakdown by customers’ kinds and types.

# COMMAND ----------

# MAGIC %sql
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
# MAGIC   ORDER BY type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM total_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH total_customer AS (
# MAGIC   SELECT
# MAGIC     c.type,
# MAGIC     c.status,
# MAGIC     COUNT(id) AS total
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   GROUP BY
# MAGIC     c.type,
# MAGIC     c.status
# MAGIC   WITH ROLLUP
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM total_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Get a list of all customers in the system breakdown by their kinds and types.

# COMMAND ----------

print('addresses:', spark.read.format("delta").load(f"{silver}/addresses"))
print('customers:', spark.read.format("delta").load(f"{silver}/customers"))
print('items:', spark.read.format("delta").load(f"{silver}/items"))
print('orders:', spark.read.format("delta").load(f"{silver}/orders"))
print('order_details:', spark.read.format("delta").load(f"{silver}/order_details"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   type,
# MAGIC   status,
# MAGIC   array_agg(id) AS ids
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.customers
# MAGIC GROUP BY
# MAGIC   type,
# MAGIC   status

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Get a list of affiliate customers who made less than 5 orders in a week. 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT CAST('2024-04-27' AS DATE) AS e_date
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   (e_date - (dayofweek(e_date) + 5) % 7) AS monday_of_week,
# MAGIC   (e_date - (dayofweek(e_date) + 5) % 7 + 7) AS sunday_of_week
# MAGIC FROM cte

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
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
# MAGIC
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

# MAGIC %md
# MAGIC ### 6. Get a list of VIP customers and the amount that was paid to each of them as a coupon discount. 

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %md
# MAGIC ### 7. Find out the average delivery time to a specific city and the average number of orders to specified cities. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.city,
# MAGIC   AVG(DATE_DIFF(o.delivered_on, o.created_on)) AS avg_delivery_day
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON a.id = o.address_id
# MAGIC GROUP BY
# MAGIC   a.city
# MAGIC ORDER BY
# MAGIC   avg_delivery_day ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Get the overall number and share of orders, delivery time of which exceeds 7 days as well as broken down by the order type. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.type,
# MAGIC   COUNT(*) FILTER(WHERE DATE_DIFF(o.delivered_on, o.created_on) > 7) AS 7days_cnt,
# MAGIC   COUNT(*) AS cnt,
# MAGIC   ROUND(100 * (COUNT(*) FILTER(WHERE DATE_DIFF(o.delivered_on, o.created_on) > 7))::NUMERIC / COUNT(*), 2) AS perc
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   ON o.customer_id = c.id
# MAGIC GROUP BY
# MAGIC   c.type
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. For each state-capital city pair get the total number of orders and the number of orders for the whole state.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH city_total_orders AS (
# MAGIC   SELECT
# MAGIC   a.state,
# MAGIC   a.city,
# MAGIC   COUNT(o.id) AS total_orders,
# MAGIC   SUM(COUNT(o.id)) OVER(PARTITION BY a.state) AS total_orders_by_state
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON a.id = o.address_id
# MAGIC   GROUP BY
# MAGIC   a.state,
# MAGIC   a.city
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC state,
# MAGIC city,
# MAGIC total_orders,
# MAGIC total_orders_by_state
# MAGIC FROM city_total_orders
# MAGIC WHERE (state, city) IN (SELECT state, city FROM hive_metastore.zhastay_yeltay_02_silver.state_capital_cities)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Get complete information on the order and the customer by the given order number

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON a.id = o.address_id
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC   ON o.customer_id = c.id
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Get a list of orders for each of the states for one month. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.state,
# MAGIC   EXTRACT(YEAR FROM o.created_on) AS order_year,
# MAGIC   EXTRACT(MONTH FROM o.created_on) AS order_month,
# MAGIC   COUNT(o.id) AS total_orders
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON a.id = o.address_id
# MAGIC GROUP BY
# MAGIC   a.state,
# MAGIC   order_year,
# MAGIC   order_month
# MAGIC ORDER BY
# MAGIC   order_year,
# MAGIC   order_month,
# MAGIC   a.state

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Get the number and list of orders made by each affiliate customer. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   c.id AS customer_id,
# MAGIC   COUNT(o.id) AS total_order
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC   ON o.customer_id = c.id
# MAGIC WHERE
# MAGIC   c.type = 'Affiliate'
# MAGIC GROUP BY
# MAGIC   c.id
# MAGIC ORDER BY
# MAGIC   c.id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Define the number of orders that were delivered to a specific customer for the entire time and for the certain period. 

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %md
# MAGIC ### 14. Get a list and the total number of orders shipped on a specific day in a specific city.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.city,
# MAGIC     o.delivered_on,
# MAGIC     COUNT(*) AS daily_total_orders,
# MAGIC     COUNT(*) OVER(PARTITION BY a.city) AS total_orders
# MAGIC FROM hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC     ON o.address_id = a.id
# MAGIC GROUP BY
# MAGIC     a.city,
# MAGIC     o.delivered_on
# MAGIC ORDER BY
# MAGIC     a.city,
# MAGIC     o.delivered_on
# MAGIC