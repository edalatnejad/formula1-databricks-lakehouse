# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

races_df =(
     spark
     .table(bronze_table)
     .filter(F.col("batch_id") == v_batch_id)
)
     # Databricks notebook source

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")

)# Databricks notebook source

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
            "date": "race_date"
        })
)# Databricks notebook source

# COMMAND ----------

display(races_renamed_df)# Databricks notebook source

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])# Databricks notebook source

# COMMAND ----------

display(races_distinct_df)# Databricks notebook source

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)# Databricks notebook source

# COMMAND ----------

display(races_final_df)# Databricks notebook source

# COMMAND ----------

# (
#     races_final_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(silver_table)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(silver_table))# Databricks notebook source

# COMMAND ----------

(
    write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,  
    merge_condition=" t.season=s.season and t.round=s.round "  ,
    columns_to_update=[
    "race_name",
    "race_date",
    "circuit_id",
    "ingestion_timestamp",
    "source_file",
    "batch_id",
    "updated_timestamp"]
    )
)
# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")