# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"# Databricks notebook source

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
         .filter(F.col("batch_id") == v_batch_id)
)# Databricks notebook source

# COMMAND ----------


races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
         .filter(F.col("batch_id") == v_batch_id)
)# Databricks notebook source

# COMMAND ----------

dim_races_df = (
            races_df
                .join(
                    circuits_df,
                    races_df.circuit_id == circuits_df.circuit_id,
                    "inner"
                )
                .select (
                    races_df.season,
                    races_df.round,
                    races_df.race_name,
                    races_df.race_date,
                    circuits_df.circuit_name,
                    circuits_df.locality,
                    circuits_df.country
                )
        )# Databricks notebook source

# COMMAND ----------

display(dim_races_df)# Databricks notebook source

# COMMAND ----------

write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)# Databricks notebook source

# COMMAND ----------

display(spark.table(target_table))