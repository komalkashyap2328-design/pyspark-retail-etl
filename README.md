<h1 align="center">End-to-End PySpark Retail ETL Pipeline</h1>

<p align="center">
  <b>Data Engineering ETL Project using PySpark, Python, Pandas, and CSV Outputs</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" />
  <img src="https://img.shields.io/badge/ETL-Pipeline-2E8B57?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Data%20Quality-Checks-purple?style=for-the-badge" />
</p>

---

## Project Summary

This project demonstrates an end-to-end ETL pipeline built with PySpark. The pipeline extracts raw retail sales data, applies data cleaning and transformation logic, performs business aggregations, validates record counts, and loads final analytical outputs as CSV files.

The project is designed to show practical Data Engineering skills in a clean and explainable way.

## Tech Stack

- Python
- PySpark
- Pandas
- CSV
- Git and GitHub
- Logging

## Project Architecture

Raw CSV Data  
→ PySpark ETL Pipeline  
→ Data Cleaning and Transformation  
→ Business Aggregations  
→ Data Quality Report  
→ Final CSV Outputs

## Project Structure

pyspark-retail-etl/

- config/
  - pipeline_config.json
- data/
  - data.csv
- images/
  - pipeline_success.png
  - output_files.png
- logs/
  - etl_pipeline.log
- output/
  - top_products.csv
  - country_revenue.csv
  - monthly_revenue.csv
  - data_quality_report.csv
- scripts/
  - etl_pipeline.py
- README.md
- requirements.txt

## Dataset

The dataset contains online retail transaction records with fields such as:

- Invoice number
- Product description
- Quantity
- Unit price
- Customer ID
- Country
- Invoice date

Input file:

- data/data.csv

## ETL Process

### 1. Extract

The pipeline reads raw retail transaction data from a CSV file using PySpark.

### 2. Transform

The transformation layer performs the following steps:

- Calculates revenue using Quantity multiplied by UnitPrice
- Removes null records
- Removes duplicate records
- Filters cancelled or negative quantity orders
- Filters invalid zero or negative unit prices
- Converts invoice date into timestamp format
- Creates an InvoiceMonth column for monthly trend analysis
- Aggregates revenue by product
- Aggregates revenue by country
- Aggregates revenue by month

### 3. Data Quality Checks

The pipeline creates a data quality report that tracks:

- Total raw records
- Total cleaned records
- Total removed records after cleaning

### 4. Load

The processed datasets are saved as CSV files in the output folder.

## Business Questions Answered

1. Which products generated the highest revenue?
2. Which countries generated the highest revenue?
3. How does revenue trend month by month?
4. How many records were removed during data cleaning?

## Output Files

| File | Description |
|---|---|
| top_products.csv | Products ranked by total revenue |
| country_revenue.csv | Countries ranked by total revenue |
| monthly_revenue.csv | Revenue aggregated by month |
| data_quality_report.csv | Raw, cleaned, and removed record counts |

## Screenshots

### Pipeline Execution

![Pipeline Execution](images/pipeline_success.png)

### Output Files

![Output Files](images/output_files.png)

## How To Run

Install dependencies:

- pip install -r requirements.txt

Run the ETL pipeline:

- python scripts/etl_pipeline.py

## Key Skills Demonstrated

- End-to-end ETL pipeline development
- PySpark DataFrame transformations
- Data cleaning and filtering
- Business-level aggregation
- Data quality reporting
- Logging and error handling
- GitHub project documentation

## Project Explanation

This project reads raw retail transaction data, cleans invalid records, calculates revenue, creates monthly trends, generates business-level summary outputs, and saves the final datasets for reporting and analysis.

The pipeline is written in a structured way using functions so each ETL step is easy to understand, maintain, and explain.

## Future Improvements

- Add Airflow orchestration for scheduling.
- Store output data in Parquet format.
- Add SQL-based reporting layer.
- Add cloud storage integration such as AWS S3.

## Final Result

The project converts raw retail sales data into clean, structured, business-ready analytics outputs using PySpark.
