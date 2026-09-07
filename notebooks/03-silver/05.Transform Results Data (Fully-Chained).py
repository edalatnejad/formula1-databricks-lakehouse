# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
catalog_name = dbutils.widgets.get("p_batch_id")
# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers# Databricks notebook source

# COMMAND ----------


(
    write_to_silver(
    input_df=results_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id AND t.constructor_id=s.constructor_id AND t.season=s.season AND t.round= s.round ",
    columns_to_update=["race_date",
    "race_name",
    "grid_position",
    "completed_laps",
    "car_number",
    "points",
    "final_position",
    "final_position_text",
    "status",
    "ingestion_timestamp",
    "source_file",
    "batch_id",
    "updated_timestamp"]
    )
 )
 

# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

results_df = (
  spark.table(bronze_table)
       .select("season",
                "round",
                "constructorId",
                "driverId",
                "date",
                "raceName",
                "grid",
                "laps",
                "number",
                "points",
                "position",
                "positionText",
                "status",
                "ingestion_timestamp",
                "source_file",
                "batch_id")
       .withColumnsRenamed({
                "constructorId": "constructor_id",
                "driverId": "driver_id",
                "raceName": "race_name",
                "date": "race_date",
                "grid": "grid_position",
                "laps": "completed_laps",
                "number": "car_number",
                "position": "final_position",
                "positionText": "final_position_text"
        })
       .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull() 
        )
       .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
       .withColumn('race_name', F.initcap(F.col("race_name")))
)# Databricks notebook source

# COMMAND ----------

# (
#     results_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(silver_table)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(silver_table))