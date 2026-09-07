# Databricks notebook source

# COMMAND ----------


if not v_batch_id:
    raise Exception("p_batch_id is missing")

print(f"Marking batch {v_batch_id} as failed")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, control_table)

source_df = (
    spark.createDataFrame(
        [(v_batch_id,)],
        ["batch_id"]
    )
    .withColumn("status", F.lit("failed"))
    .withColumn("updated_timestamp", F.current_timestamp())
)

(
    delta_table.alias("t")
    .merge(
        source_df.alias("s"),
        """
        t.batch_id = s.batch_id
        AND t.status = 'in_progress'
        """
    )
    .whenMatchedUpdate(
        set={
            "status": "s.status",
            "updated_timestamp": "s.updated_timestamp"
        }
    )
    .execute()
)

print(f"Batch {v_batch_id} marked as failed")# Databricks notebook source

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config# Databricks notebook source

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

control_table = f"{catalog_name}.{control_schema}.batch_control"# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
