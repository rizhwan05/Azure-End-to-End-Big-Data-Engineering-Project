# Azure End-to-End Big Data Engineering Project

## Overview
An end-to-end Azure data engineering solution built around the Brazilian E-Commerce (Olist) dataset. The project demonstrates modern lakehouse architecture patterns using:

- Azure Data Factory (ADF) for ingestion & orchestration
- Azure Data Lake Storage Gen2 (Bronze, Silver, Gold zones)
- Databricks (PySpark) for transformation & enrichment (including MongoDB NoSQL join)
- Azure Synapse Analytics for serving, SQL exploration, and external tables
- Optional: Power BI / Dashboards downstream (future enhancement)

## High-Level Architecture
![High-Level Architecture](Architecture%20Diagram%20(1).png)

### Layered Zones
| Layer | Purpose | Example Contents |
|-------|---------|------------------|
| Bronze | Raw immutable ingested files | Original CSVs from source (Olist datasets) |
| Silver | Cleaned & conformed data | Typed, deduplicated, joined parquet tables |
| Gold | Aggregations / curated views | Synapse external tables & views for analytics |

### Data Flow Summary
1. Source Data (CSV + MongoDB collection) is defined in `ForEachInput.json` for ADF pipeline iteration.
2. ADF ForEach copies raw CSVs into the Bronze container of Data Lake.
3. Databricks notebook (`Databricks_Code_For_Transformation.py`) mounts/authenticates to the lake, reads Bronze CSVs.
4. Cleansing & typing: duplicates dropped, schema normalization, date casting, numeric casting.
5. Derives delivery SLA metrics: actual vs estimated vs delay.
6. Joins relational entities (orders, payments, items, products, sellers, customers, geolocation) and enriches with MongoDB product category mapping.
7. Writes unified conformed dataset as Parquet to Silver (`/silver`).
8. Synapse SQL script (`Synapse_query.sql`) reads Silver parquet via `OPENROWSET`, defines Gold schema & views, and creates external tables for served analytics.
9. Downstream consumption (BI / ML) targets Gold layer.

## Azure Data Factory Pipeline
![ADF Pipeline](ADF%20pipeline.png)

The ADF pipeline uses a ForEach activity driven by the JSON list (`ForEachInput.json`) to parameterize ingest of multiple dataset files. Each iteration performs:

- Source definition (GitHub/raw storage / blob reference)
- Copy activity into Data Lake (Bronze)
- (Optionally extendable) validation / schema check

## Technologies & Services
- Azure Data Factory: Orchestration & ingestion
- Azure Data Lake Storage Gen2: Central lakehouse storage
- Databricks (Spark): Transformation, enrichment, joins, DataFrame operations
- MongoDB Atlas: NoSQL enrichment for product categories
- Azure Synapse Analytics: SQL querying, external tables, gold layer exposure
- (Future) Power BI / Azure Analysis Services: Semantic layer & reporting

## Repository Structure
```
Databricks_Code_For_Transformation.py   # PySpark transformation & enrichment logic
DataIngestionToSQL.ipynb                # Notebook exploration (optional interactive steps)
ForEachInput.json                       # ADF pipeline file iteration configuration
Synapse_query.sql                       # Synapse SQL for gold schema & external tables
data/                                   # Local copy of Olist CSV datasets
```

## Databricks Transformation Highlights
Key operations in `Databricks_Code_For_Transformation.py`:
- OAuth-based secure access to ADLS (client id, secret via env var `AZURE_CLIENT_SECRET`)
- CSV ingestion with header inference
- MongoDB integration using `pymongo` (URI injected via env var `MONGODB_URI`)
- Data cleansing: `dropDuplicates()`, `na.drop('all')`
- Type casting & date parsing with `to_date`
- Delivery performance metrics: `actual_delivery_time`, `estimated_delivery_time`, `delay_time`
- Relational joins across orders, items, products, sellers, customers, payments
- Category enrichment from MongoDB, duplicate column pruning
- Write to Silver zone as Parquet (idempotent overwrite)

### Sample Metric Logic
```python
orders_df = orders_df.withColumn("actual_delivery_time", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
										 .withColumn("estimated_delivery_time", datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))) \
										 .withColumn("delay_time", col("actual_delivery_time") - col("estimated_delivery_time"))
```

## Synapse Gold Layer
`Synapse_query.sql` illustrates:
- Ad-hoc exploration via `OPENROWSET` on Silver parquet
- Creation of Gold schema & curated views (`gold.final`, `gold.final2` filtered to delivered orders)
- External table creation referencing Gold location with defined file format (Parquet + Snappy compression)

## Security & Secrets Handling
Do NOT commit secrets. Use environment variables or secret scopes:
- `AZURE_CLIENT_SECRET` for Data Lake OAuth
- `MONGODB_URI` for MongoDB connection
- Managed Identity (Synapse) for external data source credentials where possible

## How to Run Locally (Simplified)
```powershell
# 1. Set required environment variables (replace values securely)
$Env:AZURE_CLIENT_SECRET = "<your-client-secret>"
$Env:MONGODB_URI = "<your-mongodb-uri>"

# 2. Launch Databricks notebook or run the .py inside a Databricks job
# (Upload Databricks_Code_For_Transformation.py to a workspace notebook)

# 3. After Silver write completes, run Synapse SQL script sections
# (Execute in Synapse Studio Query window)
```

## Future Enhancements
- Incorporate Delta Lake format & Change Data Capture (CDC)
- Add automated unit tests for transformation logic (e.g., schema validation)
- Implement Data Quality rules (Great Expectations / Deequ)
- Add orchestration for Databricks job trigger post-ingestion in ADF
- Publish Power BI dashboard referencing Gold external table

## Mermaid Sequence (Logical Flow)
```mermaid
flowchart LR
	A[Sources: CSV + MongoDB] --> B[ADF Ingestion ForEach]
	B --> C[Bronze (Raw CSV)]
	C --> D[Databricks Transform]
	D --> E[Silver (Clean Parquet)]
	E --> F[Synapse Views / External Tables]
	F --> G[Gold Layer Consumption]
	G --> H[BI / Analytics]
```

## Dataset Attribution
Data from the Olist Brazilian E-Commerce public dataset. Ensure compliance with dataset licensing and usage terms.

## Contributing
Feel free to open issues or PRs for improvements (testing, Delta adoption, cost optimization, monitoring).

---
Last updated: 2025-11-09