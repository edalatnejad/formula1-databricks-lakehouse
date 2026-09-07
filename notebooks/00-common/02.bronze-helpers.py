# Databricks notebook source

# COMMAND ----------

def write_to_bronze(inpute_df, target_table,batch_id):

    df_final = inpute_df.withColumn('batch_id',F.lit(batch_id ))
    ( 
        df_final
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwritewhere',f"batch_id={batch_id}")
        .option('overwriteSchema','true')
        .saveAsTable(target_table)
    )# Databricks notebook source

# COMMAND ----------

# Helper function to add the file metadata for ingestion (source file and ingestion timestamp)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn('ingestion_timestamp', F.current_timestamp())
          .withColumn('source_file', F.col('_metadata.file_path'))
    )