# Databricks notebook source

# COMMAND ----------

#circuits_final_df.withColumn('batch_id',F.lit(v_batch_id ))# Databricks notebook source

# COMMAND ----------

%skip
print (f"{source_file},{table_name}")# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id=dbutils.widgets.get("p_batch_id")# Databricks notebook source

# COMMAND ----------

print (v_batch_id)# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers# Databricks notebook source

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"# Databricks notebook source

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId',   StringType()),
    StructField("url",         StringType()),
    StructField("circuitName", StringType()),
    StructField("lat",         DoubleType()),
    StructField("long",        DoubleType()),
    StructField("locality",    StringType()),
    StructField("country",     StringType())
])# Databricks notebook source

# COMMAND ----------

circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(circuits_schema)
         .load(source_file)
)# Databricks notebook source

# COMMAND ----------

display(circuits_df)# Databricks notebook source

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)# Databricks notebook source

# COMMAND ----------

display(circuits_final_df)# Databricks notebook source

# COMMAND ----------

# (
#     circuits_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .option('overwritewhere',f"batch_id={v_batch_id}")
#         .saveAsTable(table_name)
# )# Databricks notebook source

# COMMAND ----------

display(spark.table(table_name))# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog 'formula1_incr'# Databricks notebook source

# COMMAND ----------

circuits_final_df.printSchema()# Databricks notebook source

# COMMAND ----------

write_to_bronze(
    inpute_df= circuits_final_df,
    target_table=table_name,
    batch_id=v_batch_id
    )