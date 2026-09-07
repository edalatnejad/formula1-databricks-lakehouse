# Databricks notebook source

# COMMAND ----------

 
dbutils.widgets.text('p_batch_id', '')
v_batch_id=dbutils.widgets.get('p_batch_id')# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers# Databricks notebook source

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"# Databricks notebook source

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

races_schema = StructType([
    StructField('season',   IntegerType()),
    StructField("round",    IntegerType()),
    StructField("url",      StringType()),
    StructField("raceName", StringType()),
    StructField("date",     DateType()),
    StructField("circuitId", StringType())
])# Databricks notebook source

# COMMAND ----------

races_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(races_schema)
         .load(source_file)
)# Databricks notebook source

# COMMAND ----------

display(races_df)# Databricks notebook source

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)# Databricks notebook source

# COMMAND ----------

display(races_final_df)# Databricks notebook source

# COMMAND ----------

write_to_bronze(
    inpute_df = races_final_df,
    target_table = table_name,
    batch_id=v_batch_id
)# Databricks notebook source

# COMMAND ----------

display(spark.table(table_name))