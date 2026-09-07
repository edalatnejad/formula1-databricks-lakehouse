# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC   sELECT r.season,
# MAGIC         d.driver_id,
# MAGIC         d.driver_name,
# MAGIC         d.nationality,
# MAGIC         COUNT(*) AS race_starts,
# MAGIC         SUM(r.points) AS total_points,
# MAGIC         COUNT_IF(r.is_win) AS number_of_wins,
# MAGIC         COUNT_IF(r.is_podium) AS number_of_podiums
# MAGIC     FROM formula1.gold.fact_session_results r
# MAGIC     JOIN formula1.gold.dim_drivers d
# MAGIC       ON r.driver_id = d.driver_id 
# MAGIC   GROUP BY r.season,
# MAGIC         d.driver_id,
# MAGIC         d.driver_name,
# MAGIC         d.nationality
# MAGIC     HAVING driver_id in  ('gasly','doohan') and season=2025
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC  # Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) c from formula1.silver.sprints  where season=2025 and driver_id in ('gasly','doohan') group by driver_id,season# Databricks notebook source

# COMMAND ----------

%skip
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
df_driver_standing=(
    df_fact_session_results
    .groupBy('driver_id','season')
    .agg(F.sum('points').alias('total_points')).orderBy(F.desc('total_points'))
)
display(df_driver_standing)

 # Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE  VIEW formula1.gold.vw_driver_standing
# MAGIC AS
# MAGIC WITH CTE_driver_standing AS (
# MAGIC SELECT
# MAGIC   driver_id,
# MAGIC   SUM(points) AS total_points,
# MAGIC   SUM(CASE WHEN final_position = 1 THEN 1 ELSE 0
# MAGIC SELECT * FROM  formula1.gold.dim_drivers
# MAGIC  
# MAGIC  # Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.gold.vw_driver_standing