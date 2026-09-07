# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
 # Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

drivers_df =(
     spark
     .table(bronze_table)
     .filter(F.col("batch_id") == v_batch_id)
)# Databricks notebook source

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(F.col("url"))# Databricks notebook source

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({
            "driverId": "driver_id",
            "dateOfBirth": "date_of_birth"
        })
)# Databricks notebook source

# COMMAND ----------

display(drivers_renamed_df)# Databricks notebook source

# COMMAND ----------

drivers_concatenated_df = (
  drivers_renamed_df
       .withColumn("driver_name", 
                   F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
       .drop("name")
)# Databricks notebook source

# COMMAND ----------

display(drivers_concatenated_df)# Databricks notebook source

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])# Databricks notebook source

# COMMAND ----------

display(drivers_distinct_df)# Databricks notebook source

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)# Databricks notebook source

# COMMAND ----------

display(drivers_final_df)# Databricks notebook source

# COMMAND ----------

# (
#     drivers_final_df
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
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=["date_of_birth","nationality","ingestion_timestamp","source_file","batch_id","driver_name","updated_timestamp"]
    )
 )
 
