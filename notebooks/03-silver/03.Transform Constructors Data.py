# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id = dbutils.widgets.get('p_batch_id')
# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F# Databricks notebook source

# COMMAND ----------

constructors_df =(
     spark.table(bronze_table)
     .filter(F.col("batch_id") == v_batch_id)
)
# Databricks notebook source

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")# Databricks notebook source

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)# Databricks notebook source

# COMMAND ----------

%skip
display(constructors_renamed_df)# Databricks notebook source

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])# Databricks notebook source

# COMMAND ----------

%skip
display(constructors_distinct_df)# Databricks notebook source

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)# Databricks notebook source

# COMMAND ----------

%skip
display(constructors_final_df)# Databricks notebook source

# COMMAND ----------

# (
#     constructors_final_df
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
    input_df=constructors_final_df,
    target_table=silver_table,  
    merge_condition=" t.constructor_id=s.constructor_id"  ,
    columns_to_update=[
    "constructor_name",
    "nationality",
    "ingestion_timestamp",
    "source_file",
    "batch_id",
    "updated_timestamp"]
    )
)
# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers