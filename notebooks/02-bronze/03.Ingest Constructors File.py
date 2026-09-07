# Databricks notebook source

# COMMAND ----------

write_to_bronze(
    inpute_df= constructors_final_df,
    target_table=table_name,
    batch_id=v_batch_id
    )# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers# Databricks notebook source

# COMMAND ----------

# Define source_file and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"# Databricks notebook source

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING
                         """# Databricks notebook source

# COMMAND ----------

# Read data from the constructors file
constructors_df = (
    spark.read
       .format('json')
       .schema(constructors_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)# Databricks notebook source

# COMMAND ----------

display(constructors_df)# Databricks notebook source

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)# Databricks notebook source

# COMMAND ----------

# (
#     constructors_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .option("overwriteSchema", "true")
#         .saveAsTable(table_name)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(table_name))# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text('p_batch_id','')
v_batch_id=dbutils.widgets.get('p_batch_id')