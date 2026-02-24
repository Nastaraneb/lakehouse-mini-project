# Lakehouse Analytics Pipeline

## Overview

This project implements a modular **Bronze → Silver → Gold lakehouse architecture** using Python, Pandas, Parquet, and DuckDB.

The pipeline ingests raw event, subscription, and marketing datasets, applies deterministic cleaning and validation logic, and materializes analytics-ready fact and dimension tables for business reporting.

DuckDB serves as the final query layer, allowing full ANSI SQL access to all KPIs without requiring an external database server.

---

## Architecture

### Bronze Layer
- Raw data stored exactly as received  
- All columns cast to string  
- No transformations applied  
- Ensures full raw data preservation  

### Silver Layer
- Type normalization  
- Timestamp parsing (UTC)  
- Deterministic deduplication (`event_id` based)  
- Conflict resolution (last-write-wins)  
- Quarantine handling for invalid records  
- Clean, typed Parquet outputs  

### Gold Layer
- Business metrics materialized as fact tables  
- Supporting dimension tables  
- Loaded into DuckDB under the `analytics` schema  
- Fully queryable via SQL  

---

## Implemented KPIs

- **Daily Active Users (DAU)**
- **Daily Revenue (Gross & Net)**
- **Monthly Recurring Revenue (MRR – computed daily)**
- **Weekly Cohort Retention**
- **Customer Acquisition Cost (CAC by channel)**
- **Lifetime Value (LTV per user)**
- **LTV:CAC Ratio**

All KPIs are materialized as Parquet and registered in DuckDB.

---

## Key Design Decisions

- **Parquet storage** for efficient columnar analytics
- **Full-refresh pipeline** for deterministic idempotency
- **Deterministic duplicate resolution**
- **Structured logging for observability**
- **Forward-compatible schema handling**
- **No Iceberg/Delta** (not required for this batch scope)

---

## How to Run

```bash
# Setup requirements

pandas>=2.0.0
duckdb>=0.9.0
pyarrow>=14.0.0
python-dateutil>=2.8.2

# Bronze
python -m src.bronze.events
python -m src.bronze.marketing
python -m src.bronze.subscriptions

# Silver
python -m src.silver.events
python -m src.silver.marketing
python -m src.silver.subscriptions

# Gold Metrics
python -m src.gold.metrics_basic
python -m src.gold.mrr
python -m src.gold.cohort_retention
python -m src.gold.cac
python -m src.gold.ltv
python -m src.gold.ltv_cac_ratio

# Load into DuckDB
python -m src.gold.load_to_duckdb
python -m src.gold.build_dims
