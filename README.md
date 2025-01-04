# ETL Pipeline Project: Largest Banks by Market Capitalization  

## Overview  
This project is a small-scale ETL (Extract, Transform, Load) pipeline that extracts data on the top 10 largest banks in the world by market capitalization, transforms the data to include conversions to multiple currencies, and loads the processed data into both a local CSV file and an SQL database for further analysis.  

The project showcases foundational ETL skills, including web scraping, data manipulation using Python and Pandas, and database integration, and demonstrates an understanding of data engineering concepts.  

## Tools and Technologies Used  
- **Programming Language**: Python  
- **Libraries**:  
  - `BeautifulSoup` for web scraping  
  - `Pandas` for data manipulation and transformation  
- **Database**: SQL (SQLite used locally)  

## Project Workflow  

### 1. Extract  
- Scraped data from Wikipedia for the list of the top 10 largest banks in the world by market capitalization.  
- Collected data points such as bank name, market capitalization, and country of origin.  

### 2. Transform  
- Added exchange rates for currencies (e.g., EUR, INR) from a separate CSV file.  
- Calculated and added columns for market capitalization in USD, EUR, and INR.  
- Cleaned and validated data for consistency and accuracy.  

### 3. Load  
- Saved the transformed data into a local CSV file for archival and reference.  
- Imported the processed data into a SQL database, creating a structured table.  
- Verified data integrity by running SQL queries on the database.  

## Features  
- **Web Scraping**: Automated data extraction from web sources.  
- **Data Transformation**: Enhanced raw data by adding meaningful columns and performing currency conversions.  
- **Data Storage**: Dual storage approach using CSV files and an SQL database.  
- **Data Analysis**: Sample SQL queries to demonstrate usability. 
