# Databricks notebook source

# COMMAND ----------

%fs ls 'abfss://<your-email>/landing'# Databricks notebook source

# COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1_incr
URL 'abfss://<your-email>/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1-incr container';# Databricks notebook source

# COMMAND ----------

SHOW CATALOGS;# Databricks notebook source

# COMMAND ----------


CREATE CATALOG IF NOT EXISTS formula1_incr
   COMMENT 'This is the main catalog for the formula1_incrementalLoad project' ;# Databricks notebook source

# COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze;
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver;
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold;
       # Databricks notebook source

# COMMAND ----------

SELECT current_catalog();# Databricks notebook source

# COMMAND ----------

USE CATALOG formula1_incr;# Databricks notebook source

# COMMAND ----------

SHOW SCHEMAS;# Databricks notebook source

# COMMAND ----------

CREATE VOLUME IF NOT EXISTS formula1_incr.landing.files;# Databricks notebook source

# COMMAND ----------

%fs ls /Volumes/formula1_incr/landing/files