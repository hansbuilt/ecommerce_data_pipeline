import shopify_gen as sho
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import os


def get_timestamp_prefix():
    """Returns a timestamp prefix string for use as a prefix on filenames, in the format YYYYMMDD_hhmmss"""

    timestamp_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
    return timestamp_prefix


def extract_allproducts_csv(fileloc="../data_raw/"):
    """Creates a raw csv extract of the all products GET request."""

    df = sho.get_all_products_df()

    prefix = get_timestamp_prefix()

    filefullpath = fileloc + prefix + " shopify_allproducts.csv"

    df.to_csv(filefullpath, index=False)

    print(f"File created: {filefullpath}")


file_path = "../data_raw/20251104_212802 shopify_allproducts.csv"


def load_to_bigquery(file_path):

    project = os.getenv("gcp_bigquery_project_name")

    client = bigquery.Client(project=project)

    df = pd.read_csv(file_path)

    # quick edit to test upload
    df.columns = df.columns.str.replace(".", "_", regex=False)

    client.load_table_from_dataframe(
        df, project + ".raw.products_raw"
    ).result()
    print(f" Loaded {file_path} to BigQuery")
