"""
https://docs.cloud.google.com/sdk/docs/install

"""

from google.cloud import bigquery

import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

project = os.getenv("gcp_bigquery_project_name")

client = bigquery.Client(project=project)


def get_last_order_updatedt():
    """Returns a datetime of the max updated_at date from the raw table of order data."""

    dataset = "raw"
    table = "orders_raw"

    query = f"""
        SELECT MAX(updated_at) AS max_updated_at
        FROM `{project}.{dataset}.{table}`
    """

    # test query
    # query = "SELECT 'Hello, BigQuery!' AS greeting"
    df = client.query(query).to_dataframe()

    result = df["max_updated_at"][0]

    result_formatted = datetime.fromisoformat(result)

    return result_formatted
