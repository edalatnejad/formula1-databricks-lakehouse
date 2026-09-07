# Databricks notebook source

# COMMAND ----------

write_to_bronze(
    inpute_df= drivers_final_df,
    target_table=table_name,
    batch_id=v_batch_id
    )# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id=dbutils.widgets.get('p_batch_id')# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers# Databricks notebook source

# COMMAND ----------

# Define source_file and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"# Databricks notebook source

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])# Databricks notebook source

# COMMAND ----------

# Read data from the drivers file
drivers_df = (
    spark.read
       .format('json')
       .schema(drivers_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)# Databricks notebook source

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)# Databricks notebook source

# COMMAND ----------

# (
#     drivers_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .option("overwriteSchema", "true")
#         .saveAsTable(table_name)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(table_name))