# Customer-Shopping-Trends-ETL-Pipeline
An end-to-end ETL pipeline using Apache Airflow, PostgreSQL, Elasticsearch, Great Expectations, and Kibana to automate customer shopping data processing, validation, and visualization.

# Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline to process customer shopping data using Apache Airflow, PostgreSQL, and Elasticsearch. It also incorporates data quality validation using Great Expectations and interactive data visualization through Kibana.
The project demonstrates how automated data pipelines can transform raw transactional data into structured, validated, and analysis-ready datasets to support data-driven business decisions.

# Problem Background

Understanding customer shopping behavior is essential for retail businesses to improve sales performance, customer satisfaction, and marketing strategies.
Customer transaction data contains valuable information about purchasing patterns, product preferences, and customer demographics. However, raw datasets require proper data cleaning, validation, and processing before they can be effectively used for analysis.
This project addresses these challenges by implementing an automated data pipeline that ensures data consistency, reliability, and accessibility.

# Project Objectives

- Develop an automated ETL pipeline using Apache Airflow.

- Perform data preprocessing to improve data quality and consistency.

- Implement data validation using Great Expectations.

- Store cleaned customer shopping data in Elasticsearch.

- Develop interactive Kibana visualizations to explore customer shopping patterns.

# Dataset Information

The dataset used in this project is the **Customer Shopping Trends Dataset**, obtained from Kaggle.

- Source: Kaggle

- Dataset: Customer Shopping Trends

- Format: CSV

- Records: 3,900

- Original Features: 18

The dataset includes customer demographics, product categories, purchase amounts, payment methods, shipping preferences, and purchasing behavior.

Dataset Source: Customer Shopping Trends Dataset – Kaggle

# Methodology

**1. Data Extraction**

Raw customer shopping data is stored in PostgreSQL and extracted automatically using Apache Airflow.

**2. Data Transformation**

Data preprocessing is performed using Python and Pandas, including:

- Removing duplicate records.
- Standardizing column names.
- Handling missing values using median and mode imputation.
- Removing unnecessary columns.
- Preparing the cleaned dataset for downstream analysis.

**3. Data Loading**

The cleaned dataset is loaded into Elasticsearch, a NoSQL search and analytics engine.

**4. Data Validation**

Great Expectations is used to validate data quality through multiple expectation rules, including:

- Column uniqueness.
- Value range validation.
- Data type validation.
- Categorical value validation.
- Additional data quality checks.

**5. Data Visualization**

Kibana is used to develop visualizations that explore customer shopping patterns, including purchasing behavior, product preferences, and customer characteristics.

**ETL Pipeline Architecture**

Raw CSV Dataset → PostgreSQL → Apache Airflow (Extraction & Cleaning) → Elasticsearch → Kibana Dashboard

Great Expectations is used separately to validate the dataset and assess data quality.


# Project Outputs

1. Automated ETL Pipeline

An Apache Airflow DAG automates data extraction, cleaning, and loading.

2. Cleaned Dataset

A structured and analysis-ready dataset produced through preprocessing.

3. Data Quality Validation

Data validation checks using Great Expectations to assess consistency, completeness, and validity.

4. Elasticsearch Storage

Processed customer shopping records stored in Elasticsearch for further exploration.

5. Kibana Dashboard

Interactive visualizations supporting the exploration of customer shopping behavior.
