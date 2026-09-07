# Formula 1 Databricks Lakehouse

A hands-on data engineering project that processes Formula 1 racing data using Databricks, PySpark, Spark SQL, Delta Lake and Unity Catalog.

## Project Overview

The project implements a Medallion architecture with Bronze, Silver and Gold layers. It includes incremental batch processing, reusable transformation helpers, analytics-ready dimensional models and Databricks Jobs orchestration.

## Architecture

**Source files → Landing Volume → Bronze → Silver → Gold → Analytics**

* **Bronze:** Ingests source files into Delta tables with ingestion metadata and batch identifiers.
* **Silver:** Cleans and standardises data, applies transformations and uses Delta MERGE for incremental processing.
* **Gold:** Builds dimensions and a session-results fact table for analytics.
* **Orchestration:** Uses Databricks Jobs to identify the next batch, execute the processing pipeline and update batch status.

## Technologies

Databricks · PySpark · Spark SQL · Delta Lake · Unity Catalog · Databricks Jobs · Python · SQL

## Key Features

* Incremental batch ingestion and processing
* Delta MERGE transformations
* Batch-control tracking
* Dependency-based workflow orchestration
* Bronze, Silver and Gold data layers
* Dimensional modelling and analytics-ready datasets

## Repository Structure

```text
notebooks/
├── 00-common/
├── 01-setup/
├── 02-bronze/
├── 03-silver/
├── 04-gold/
├── 05-analytics/
└── 06-orchestration/
```

## Project Background

This project was initially developed while completing a Databricks data engineering course and adapted through personal hands-on practice. It is intended to demonstrate practical data engineering skills rather than represent a production deployment.
