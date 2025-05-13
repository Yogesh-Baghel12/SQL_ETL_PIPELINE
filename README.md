# SQL_ETL_PIPELINE

Welcome to the Data Warehouse Project repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

![Alt text](image/data_architecture.png)


1.Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2.Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

3.Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.


# 📖 Project Overview
This project involves:

Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
Data Modeling: Developing fact and dimension tables optimized for analytical queries.
Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

# Building the Data Warehouse (Data Engineering)
## Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

### Specifications

- **Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis.
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope:** Focus on the latest dataset only; historization of data is not required.
# STRUCTURE
```
SQL-ETL-PROJECT/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── image/                              # Project documentation and architecture details
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
└── requirements.txt                    # Dependencies and requirements for the project
```
