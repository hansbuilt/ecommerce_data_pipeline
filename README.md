# ecommerce_data_pipeline

Draft purpose of this project:

Create a data pipeline that:
0. generate fake data with Shopify's sandbox store api (python)
1. Pulls daily sales / customer data from a mock data source (python, airflow)
- products, variants, customers, orders, line items
2. Cleans and transforms the data (python)
3. Loads raw data into a database (BigQuery)
4. Staging for cleaning data, marts for final metrics (dbt)
6. Scheduled dataset refresh for portability
7. Scheduled connection refresh in Google Sheets to pull data from Bigquery dataset
8. Visualize using Sheets data (tableau public)

  Tableau Visuals:

<img width="2496" height="1304" alt="image" src="https://github.com/user-attachments/assets/d18b5880-fc57-442d-b3bf-fd208f1c7357" />

<img width="2464" height="1258" alt="image" src="https://github.com/user-attachments/assets/cbcd2194-8ed4-480b-8045-c873c10db9a4" />

<img width="2496" height="1304" alt="image" src="https://github.com/user-attachments/assets/bd5f9729-a8ac-4c68-9e15-8d2e831f63ba" />
