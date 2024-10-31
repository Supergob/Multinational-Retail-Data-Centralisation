# Multinational Retail Data Centralisation

This retail project aims to refine and further develop the skills and knowledge gained throughout this course. In particular, it focuses on building a comprehensive ETL pipeline for sales data from various sources, including AWS RDS databases and S3 storage.

## Key Features

- Extracts data from multiple sources, including RDS databases and S3 buckets.
- Cleans and preprocesses the data to ensure consistency and remove errors.
- Handles specific transformations, such as product weight conversion and phone number validation.
- Supports the creation of a structured database for sales analytics.

## Aim Of The Project

- **Extract Data**: Gather retail data from multiple sources, including AWS RDS databases and S3 buckets.
- **Clean Data**: Ensure data consistency by removing duplicates and handling missing values.
- **Transform Data**: Convert various data formats, such as CSV, JSON, and PDF into a standardized format.
- **Centralize Data**: Store the cleaned and transformed data in a central AWS RDS database.
- **Enable Analysis**: Establish a star-based schema to facilitate efficient data analysis and reporting.

## What I've Learned

- Working with AWS S3 for data storage and retrieval.
- Building and managing ETL pipelines.
- Data cleaning using Pandas, NumPy and regular expressions.
- Database interaction using SQLAlchemy and integration via SQLAlchemy.
- YAML configurations.

## Installation Instructions

To set up and run this project, you will need to follow these steps:

1. Cloning the repository
2. Installing the required Python Packages
3. Set up your dabasase credentials in a YAML file
4. Running the project in your environment

## Usage Instructions

1. Extract Data: Use the "DataExtractor" class to pull data from your sources (e.g., RDS, S3).
2. Clean Data: Use the DataCleaning class methods to clean and preprocess data such as customer orders, products, and stores.
3. Upload Data: Upload the cleaned data back to the database using the DatabaseConnector class.

## File Structure

RetailProject/

│

├── data_extraction.py       # Contains methods to extract data from different sources

├── data_cleaning.py         # Contains methods to clean and process data

├── database_utils.py        # Contains methods for interacting with the database

├── utilities.py             # Helper methods like reading database credentials

├── README.md                # Project description and instructions

└── requirements.txt         # List of required packages
