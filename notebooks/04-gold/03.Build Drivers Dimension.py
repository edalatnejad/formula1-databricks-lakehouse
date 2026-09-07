# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers# Databricks notebook source

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

drivers_df = (
    spark.table(f"{catalog_name}.{silver_schema}.drivers")
         .filter(F.col("batch_id") == v_batch_id)
)# Databricks notebook source

# COMMAND ----------

ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")# Databricks notebook source

# COMMAND ----------

dim_drivers_df = (
    drivers_df
        .join(
            ref_nationality_region_df,
            drivers_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
        .select(
            drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)# Databricks notebook source

# COMMAND ----------

display(dim_drivers_df)# Databricks notebook source

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)# Databricks notebook source

# COMMAND ----------

display(spark.table(target_table))