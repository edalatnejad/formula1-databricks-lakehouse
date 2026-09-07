# Databricks notebook source

# COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.control
    # Databricks notebook source

# COMMAND ----------

CREATE TABLE IF NOT EXISTS formula1.control.batch_events
(
    batch_id INT,
    event_timestamp TIMESTAMP
)# Databricks notebook source

# COMMAND ----------

INSERT INTO formula1.control.batch_events
VALUES (1, current_timestamp());# Databricks notebook source

# COMMAND ----------

INSERT INTO formula1.control.batch_events
VALUES (2, current_timestamp());# Databricks notebook source

# COMMAND ----------

SELECT * FROM formula1.control.batch_events;