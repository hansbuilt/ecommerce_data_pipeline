# ecommerce_data_pipeline

Draft purpose of this project:

Create a data pipeline that:
0. generate fake data with Shopify's sandbox store api (python)
1. Pulls daily sales / customer data from a mock data source (python, airflow)
- products, variants, customers, orders, line items
- need raw staging layer
2. Cleans and transforms the data (python)
3. Loads into a database (BigQuery)
4. Create model layer for metrics (dbt)
5. Visualize (tableau public)