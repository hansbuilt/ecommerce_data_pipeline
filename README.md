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

<img width="1112" height="642" alt="image" src="https://github.com/user-attachments/assets/b8c4d400-d06f-42d1-81c4-4a37a152435d" />

<img width="1112" height="642" alt="image" src="https://github.com/user-attachments/assets/d700bda0-0805-4376-8445-434c5de05ec3" />

<img width="1112" height="642" alt="image" src="https://github.com/user-attachments/assets/5a7ab0c7-4b36-49a0-a968-f2182219d7ce" />
