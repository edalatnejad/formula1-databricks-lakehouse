# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id=dbutils.widgets.get('p_batch_id')# Databricks notebook source

# COMMAND ----------

write_to_bronze(
    inpute_df= sprints_final_df,
    target_table=table_name,
    batch_id=v_batch_id
    )# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers# Databricks notebook source

# COMMAND ----------

# Define source_file and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"# Databricks notebook source

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

sprints_schema = StructType([
    StructField("date", StringType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])
# Databricks notebook source

# COMMAND ----------

# Read data from the sprints file
sprints_df = (
    spark.read
       .format('json')
       .schema(sprints_schema)
       .option('mode', 'FAILFAST')
       .option('multiLine', True)
       .load(source_file)
)# Databricks notebook source

# COMMAND ----------

display(sprints_df)# Databricks notebook source

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)# Databricks notebook source

# COMMAND ----------

# (
#     sprints_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .option('overwriteSchema', 'True')
#         .saveAsTable(table_name)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(table_name))