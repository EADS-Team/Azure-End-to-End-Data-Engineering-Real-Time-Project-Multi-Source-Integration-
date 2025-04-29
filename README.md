# Azure-End-to-End-Data-Engineering-Multi-Source-Integration-Project

This project demonstrates a robust and scalable data engineering pipeline in Azure that integrates diverse data sources, performs transformations, and produces a unified dataset for insightful reporting through Power BI.

## Project Overview

The objective is to build a unified data pipeline in Azure that ingests customer and sales data from multiple platforms—SQL Server, Oracle DB, GitHub, and Azure Blob Storage—into a centralized Azure Data Lake Storage (ADLS). The data is transformed into a consolidated dataset and modeled into facts and dimensions, providing stakeholders with a Power BI dashboard for advanced sales analytics.

## Business Requirements

The project aims to support the following key business goals:

  * Create a consolidated retail dataset from diverse platforms.
  * Analyze and visualize customer and sales trends.
  * Enable reporting across gender, product, geography, and sales over time.
  * Provide a clean and structured data model for downstream analytics.

## Solution Overview

## Data Ingestion

### Sources

* SQL Server (SSMS) – Sales data  
* Oracle Database – Store information  
* GitHub – Inventory and Product CSV files  
* Azure Blob Storage – Customer CSV file
  
### Process:

* Azure Data Factory (ADF) is used to copy data from all five sources.
* Copy Data and ForEach activities in ADF pipelines extract the data.
* The files are ingested into the **Bronze** container of ADLS.

## Data Transformation
 
### Bronze to Silver Layer (Notebook: bronze_to_silver)

* Azure Databricks is used to read files from the Bronze layer.
* Data transformations include:
  * Data type corrections in the Sales table.
  * Renaming columns to **snake_case** in the Stores table.
  * Joining all five source tables into a single unified table called **Retail_Dataset**.
* Old files are removed from Silver; only the latest Retail_Dataset is stored.

### Silver to Gold Layer (Notebook: silver_to_gold)

* The Retail_Dataset is further processed into:
   * **Fact Tables**: e.g., Sales_Fact
   * **Dimension Tables**: e.g., Product_Dim, Store_Dim, Customer_Dim
* These are written to the Gold container.
* Old Gold layer files are removed before writing new ones.

## Data Storage and Reporting

* The final fact and dimension tables in the Gold container are connected to Power BI.
* A dashboard is created with filters and KPIs covering:
      * Total sales
      * Sales by product category
      * Customer demographics
      * Time-based trends

## Automation

* ADF pipelines orchestrate the entire flow from ingestion to transformation.
* Databricks notebooks are integrated within the ADF pipelines.
* Pipelines are scheduled to run and refresh data regularly.

## Technology Stack

  * **Azure Data Factory (ADF)** – Data ingestion and orchestration
  * **Azure Data Lake Storage (ADLS)** – Bronze, Silver, and Gold containers
  * **Azure Databricks** – Data processing and transformation
  * **Power BI** – Visualization and reporting
  * **SQL Server & Oracle DB** – On-premise and external data sources
  * **GitHub & Azure Blob Storage** – External file sources

## Setup Instructions

### Step 1: Azure Environment Setup 

   * Create a Resource Group
   * Deploy ADF, ADLS, and Databricks
   * Create Bronze, Silver, and Gold containers in ADLS

### Step 2: Ingestion

   * Connect ADF to SQL Server, Oracle, GitHub, and Blob
   * Create pipelines with Copy Data & ForEach activities
   * Ingest files into the Bronze layer

### Step 3: Transformation

   * Mount ADLS containers in Databricks
   * Run **bronze_to_silver** notebook to:
       * Clean, transform, and consolidate into **Retail_Dataset**
   * Run **silver_to_gold** notebook to:
       * Create facts and dimensions
   * Integrate notebooks into the ADF pipeline

### Step 4: Reporting

   * Connect Power BI to the Gold layer
   * Build reports using fact and dimension tables

## Security and Monitoring

   * Azure Key Vault manages credentials securely.
   * Monitor ADF pipeline runs and Databricks job executions.

## End-to-End Testing

   * Insert test records across source systems.
   * Validate transformations and joins in Retail_Dataset.
   * Confirm Power BI dashboards reflect current data.

## Conclusion

This real-time project showcases a powerful and flexible Azure pipeline for integrating heterogeneous data sources into a unified analytical model. By using Azure’s cloud-native services, the pipeline ensures efficient ingestion, transformation, automation, and rich visualization—delivering critical insights to business stakeholders.
