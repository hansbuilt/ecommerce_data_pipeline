# ecommerce_data_pipeline

Draft purpose of this project:

Create a data pipeline that:
0. generate fake data with Shopify's sandbox store
- get api connection set
- make one time dummy order data generator in shopify or csv upload it
- make hourly? order data generator, maybe automate on web app
1. Pulls daily sales / customer data from a mock data source (python, airflow)
- at least products, customers, orders, line items, transactions, fulfillments
- need raw staging layer
2. Cleans and transforms the data (python)

3. Loads into a database (BigQuery)
4. Create model layer for metrics (dbt)
5. Visualize (tableau public)