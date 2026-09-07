# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F


dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

if not v_batch_id:
    raise Exception("p_batch_id is missing")# Databricks notebook source

# COMMAND ----------

# from pyspark.sql import Row
# from pyspark.sql import functions as F

# if v_batch_id:
#     in_progress_df = (
#         spark.createDataFrame(
#             [Row(batch_id=v_batch_id, status="in_progress")]
#         )
#         .withColumn("created_timestamp", F.current_timestamp())
#         .withColumn("updated_timestamp", F.current_timestamp())
#     )

#     (
#         in_progress_df.write
#             .format("delta")
#             .mode("append")
#             .saveAsTable(control_table)
#     )

#     print(f"Marked batch {v_batch_id} as in_progress")
# else:
#     raise Exception("batch_id is missing")  # Databricks notebook source

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

control_table = f"{catalog_name}.{control_schema}.batch_control"

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

if not v_batch_id:
    raise Exception("p_batch_id is missing")# Databricks notebook source

# COMMAND ----------

delta_table = DeltaTable.forName(spark, control_table)

source_df = (
    spark.createDataFrame([(v_batch_id,)], ["batch_id"])
    .withColumn("status", F.lit("in_progress"))
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
)# Databricks notebook source

# COMMAND ----------

(
    delta_table.alias("t")
    .merge(
        source_df.alias("s"),
        "t.batch_id = s.batch_id"
    )
    .whenMatchedUpdate(
        condition="t.status = 'failed'",
        set={
            "status": "s.status",
            "updated_timestamp": "s.updated_timestamp"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)# Databricks notebook source

# COMMAND ----------

print(f"Batch {v_batch_id} is ready for processing")