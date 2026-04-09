# Modern SQL Data Warehouse Project

Building a modern Data Warehouse using SQL Server, including ETL pipelines, Data Modeling, and Analytics.

# 📊 Project Architecture

This project follows Medallion Architecture:

* Bronze Layer — Raw data ingestion from source systems (ERP, CRM CSV files)
* Silver Layer — Data cleansing, standardization, transformation
* Gold Layer — Business-ready data model (Fact & Dimension tables)

 # Data Architecture: Designing a modern data warehouse using Medallion Architecture (Bronze, Silver, and Gold layers).
   <img width="1527" height="806" alt="data_architecture-DWH" src="https://github.com/user-attachments/assets/81688430-34ea-4f92-9af5-064c4877f482" />


# 🚀 Project Objectives

Develop a modern SQL-based Data Warehouse to:

* Consolidate ERP and CRM data
* Enable analytical reporting
* Improve data quality
* Provide business insights
* Support decision making

 # ⚙️ ETL Pipeline

Pipeline stages:

* Load raw CSV → Bronze
* Clean & transform → Silver
* Build Fact & Dimension → Gold
* Run Data Quality Checks
Audit logging for pipeline execution

# 🧱 Data Model

Gold Layer includes:

# Fact Tables
* fact_sales

# Dimension Tables
* dim_customer
* dim_product

# data model image
<img width="1500" height="558" alt="data_Model_StarSchema" src="https://github.com/user-attachments/assets/77830e63-f1f6-47da-b0ae-657636c8d812" />


Optimized for analytical queries and reporting.

# ➕ Data Integration
<img width="861" height="571" alt="Data_integration drawio" src="https://github.com/user-attachments/assets/be3c473a-5ca9-4a62-b87f-5c6d70eba0cf" />


# 📈 Analytics & Reporting

Analytics built using SQL:

* Customer behavior analysis
* Product performance metrics
* Sales trend analysis
* Revenue insights
* Top customers & products
  
# 🥅 This repositories includes
  * SQL Development
  * Data Architect
  * Data Engineering
  * ETL Pipeline Developer
  * Data Modeling
  * Data Analytics


# 🚀 Project Requirements
Building the Data Warehouse 
Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

# Specifications
* Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
* Data Quality: Cleanse and resolve data quality issues prior to analysis.
* Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
* Scope: Focus on the latest dataset only; historization of data is not required.
* Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.


 # 🖼 BI: Analytics & Reporting 
Objective
Develop SQL-based analytics to deliver detailed insights into:

* Customer Behavior
* Product Performance
* Sales Trends

These insights empower stakeholders with key business metrics, enabling strategic decision-making.




