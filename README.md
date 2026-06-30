# dlt-scd2-workshop
Workshop demonstrating SCD Type 2 implementation using Spark Declarative Pipelines (DLT) in Databricks.

# DLT SCD2 Workshop

This workshop demonstrates how to implement Slowly Changing Dimension (SCD) Type 2 using Spark Declarative Pipelines (Delta Live Tables) in Databricks.

## Repository Structure

```text
datasets/
    customers_v1.csv
    customers_v2.csv

notebooks/
    dlt_scd2_demo.py
```

## Prerequisites

- Access to Databricks Workspace
- Unity Catalog enabled
- Permission to create Pipelines

## Workshop Objectives

By the end of this workshop, you will learn:

- How to create Bronze tables using Auto Loader
- How to implement SCD Type 2 using Declarative Pipelines
- How to preserve historical changes automatically
- How Auto CDC Flow works in Databricks

## Workshop Flow

1. Upload customer CSV files to a Databricks Volume.
2. Create a Bronze table using Auto Loader.
3. Create a Silver table.
4. Implement Auto CDC Flow (SCD2).
5. Observe history tracking.

