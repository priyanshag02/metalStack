# Description

This project demonstrates the end-to-end process of designing and building a robust data warehouse solution.

## Data Architecture

The data warehouse is built using the Medallion Architecture approach, which organizes data into three logical layers:
- Bronze Layer:  Stores raw data as-is from the source systems. Data is ingested from CSV Files into PostgreSQL Server Database.
- Silver Layer:  This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
- Gold Layer:  Houses business-ready data modeled into a star schema required for reporting and analytics.

<br>

<img width="750" height="450" alt="dwh_architecture" src="https://github.com/user-attachments/assets/1aed9523-fd32-4868-ba8f-75a508133b93" />
