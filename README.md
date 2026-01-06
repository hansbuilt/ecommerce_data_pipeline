# ecommerce_data_pipeline

Draft purpose of this project:

Create a data pipeline that:
0. generate fake data with Shopify's sandbox store api (python)
1. Pulls daily sales / customer data from a mock data source (python, airflow)
- products, variants, customers, orders, line items
- need raw staging layer
2. Cleans and transforms the data (python)
3. Loads raw data into a database (BigQuery)
4. Staging for cleaning data, marts for final metrics (dbt)
6. Scheduled dataset refresh for portability
7. Scheduled connection refresh in Google Sheets to pull data from Bigquery dataset
8. Visualize using Sheets data (tableau public)