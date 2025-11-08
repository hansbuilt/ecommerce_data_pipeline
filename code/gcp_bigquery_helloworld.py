"""
https://docs.cloud.google.com/sdk/docs/install

"""

from google.cloud import bigquery

import os
from dotenv import load_dotenv

load_dotenv()

project = os.getenv("gcp_bigquery_project_name")

client = bigquery.Client(project=project)


# test query
query = "SELECT 'Hello, BigQuery!' AS greeting"
df = client.query(query).to_dataframe()

print(df)
