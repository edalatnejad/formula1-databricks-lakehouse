# Formula 1 Databricks Lakehouse

A hands-on data engineering project developed while completing Databricks training.

## Overview
The project processes Formula 1 data through Bronze, Silver and Gold layers using Databricks, PySpark, Spark SQL, Delta Lake and Unity Catalog.

## Features
- Batch-based ingestion and incremental Delta MERGE processing
- Reusable transformation helpers
- Analytics-ready dimensions and session-results fact table
- Databricks Jobs with dependency-based execution and batch-control notebooks

## Repository
The `notebooks/` directory contains the exported project notebooks. Configure the catalog and landing volume in `00-common/01.environment-config` before running in your own workspace.

## Status
The pipeline has been run successfully in Databricks. This is a learning portfolio project, not a production deployment.

## Acknowledgement
Initially developed as part of a Udemy Databricks data engineering course and adapted for personal hands-on practice.
