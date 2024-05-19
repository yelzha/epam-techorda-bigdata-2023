# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.city_geodata
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   ROUND(AVG(lat), 10) AS lat,
# MAGIC   ROUND(AVG(lng), 10) AS lng
# MAGIC FROM zhastay_yeltay_02_silver.addressline_enriched
# MAGIC WHERE 
# MAGIC   lat IS NOT NULL 
# MAGIC   AND lng IS NOT NULL
# MAGIC GROUP BY
# MAGIC   state,
# MAGIC   city

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.01v2_cities_by_vip_customer_count
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
# MAGIC   cg.lat,
# MAGIC   cg.lng,
# MAGIC   q1.city,
# MAGIC   q1.total_vip_customers
# MAGIC FROM cities_by_vip AS q1
# MAGIC INNER JOIN zhastay_yeltay_03_gold.city_geodata AS cg
# MAGIC   ON cg.city = q1.city

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.total_revenue_by_all_categories
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH all_joined AS (
# MAGIC   SELECT
# MAGIC     o.id AS order_id,
# MAGIC     o.created_on AS o_created_on,
# MAGIC     o.delivered_on,
# MAGIC     o.delivery_date,
# MAGIC     od.quantity,
# MAGIC     i.price,
# MAGIC     c.id AS customer_id,
# MAGIC     c.status,
# MAGIC     c.type,
# MAGIC     a.id AS address_id,
# MAGIC     a.created_on AS a_created_on,
# MAGIC     ae.id AS addressline_id,
# MAGIC     a.addressline,
# MAGIC     ae.completed_address,
# MAGIC     ae.postal_code,
# MAGIC     ae.neighborhood,
# MAGIC     ae.county,
# MAGIC     a.city,
# MAGIC     a.state,
# MAGIC     scc.abbreviation AS state_code,
# MAGIC     a.country,
# MAGIC     ae.lat,
# MAGIC     ae.lng,
# MAGIC     mc.metropolitan,
# MAGIC     mc.metropolitan IS NOT NULL AS is_metropolitan,
# MAGIC     scc.city = a.city AS is_state_capital_city
# MAGIC   FROM zhastay_yeltay_02_silver.orders AS o
# MAGIC   INNER JOIN zhastay_yeltay_02_silver.addresses AS a
# MAGIC     ON o.address_id = a.id
# MAGIC   INNER JOIN zhastay_yeltay_02_silver.customers AS c
# MAGIC     ON o.customer_id = c.id
# MAGIC   INNER JOIN zhastay_yeltay_02_silver.order_details AS od
# MAGIC     ON o.id = od.order_id
# MAGIC   INNER JOIN zhastay_yeltay_02_silver.items AS i
# MAGIC     ON od.item_id = i.id
# MAGIC   INNER JOIN zhastay_yeltay_02_silver.addressline_enriched AS ae
# MAGIC     ON a.country = ae.country
# MAGIC     AND a.state = ae.state
# MAGIC     AND a.city = ae.city
# MAGIC     AND a.addressline = ae.addressline
# MAGIC   LEFT JOIN zhastay_yeltay_02_silver.metropolitan_cities AS mc
# MAGIC     ON mc.state = a.state
# MAGIC     AND mc.city = a.city
# MAGIC   LEFT JOIN zhastay_yeltay_02_silver.state_capital_cities AS scc
# MAGIC     ON scc.state = a.state
# MAGIC )
# MAGIC SELECT
# MAGIC   state,
# MAGIC   state_code,
# MAGIC   status,
# MAGIC   type,
# MAGIC   is_metropolitan,
# MAGIC   is_state_capital_city,
# MAGIC   COUNT(DISTINCT order_id) AS cnt,
# MAGIC   COUNT(quantity * price) AS total_revenue
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE status = 'VIP')::NUMERIC / COUNT(DISTINCT order_id) AS vip_orders_percentage,
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE status = 'Regular')::NUMERIC / COUNT(DISTINCT order_id) AS regular_orders_percentage,
# MAGIC
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE type = 'Individual')::NUMERIC / COUNT(DISTINCT order_id) AS individual_percentage,
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE type = 'Affiliate')::NUMERIC / COUNT(DISTINCT order_id) AS affiliate_orders_percentage,
# MAGIC
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE is_metropolitan)::NUMERIC / COUNT(DISTINCT order_id) AS metropolitan_percentage,
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE NOT is_metropolitan)::NUMERIC / COUNT(DISTINCT order_id) AS not_metropolitan_percentage,
# MAGIC
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE is_state_capital_city)::NUMERIC / COUNT(DISTINCT order_id) AS state_capital_city_percentage,
# MAGIC   -- COUNT(DISTINCT order_id) FILTER(WHERE NOT is_state_capital_city)::NUMERIC / COUNT(DISTINCT order_id) AS not_state_capital_city_percentage
# MAGIC FROM all_joined
# MAGIC GROUP BY
# MAGIC   state,
# MAGIC   state_code,
# MAGIC   status,
# MAGIC   type,
# MAGIC   is_metropolitan,
# MAGIC   is_state_capital_city

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.daily_total_revenue
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.delivered_on,
# MAGIC COUNT(*) AS total_orders,
# MAGIC SUM(od.quantity * i.price) AS revenue
# MAGIC FROM zhastay_yeltay_02_silver.orders AS o
# MAGIC INNER JOIN zhastay_yeltay_02_silver.order_details AS od
# MAGIC   ON o.id = od.order_id
# MAGIC INNER JOIN zhastay_yeltay_02_silver.items AS i
# MAGIC   ON od.item_id = i.id
# MAGIC GROUP BY
# MAGIC   o.delivered_on
# MAGIC ORDER BY
# MAGIC   o.delivered_on
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.zhastay_yeltay_03_gold.customers_havent_reached_at_least_5_orders_all_weeks
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC WITH affiliate_by_weekly_orders AS (
# MAGIC   SELECT
# MAGIC     c.id AS customer_id,
# MAGIC     c.status AS customer_status,
# MAGIC     a.city,
# MAGIC     a.state,
# MAGIC     (CAST(o.created_on AS DATE) - (dayofweek(o.created_on) + 5) % 7) AS monday_of_week,
# MAGIC     COUNT(*) AS weekly_total_orders
# MAGIC   FROM hive_metastore.zhastay_yeltay_02_silver.addresses AS a
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.orders AS o
# MAGIC     ON a.id = o.address_id
# MAGIC   INNER JOIN hive_metastore.zhastay_yeltay_02_silver.customers AS c
# MAGIC     ON o.customer_id = c.id
# MAGIC   GROUP BY
# MAGIC     c.id,
# MAGIC     c.status,
# MAGIC     a.city,
# MAGIC     a.state,
# MAGIC     monday_of_week
# MAGIC )
# MAGIC SELECT
# MAGIC   city,
# MAGIC   state,
# MAGIC   customer_id,
# MAGIC   COUNT(*) FILTER(WHERE weekly_total_orders >= 5) = 0 AS reached_at_least_5_orders_all_weeks
# MAGIC FROM affiliate_by_weekly_orders
# MAGIC GROUP BY
# MAGIC   city,
# MAGIC   state,
# MAGIC   customer_id