import shopify_gen as sho
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import os


def get_timestamp_prefix():
    """Returns a timestamp prefix string for use as a prefix on filenames, in the format YYYYMMDD_hhmmss"""

    timestamp_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
    return timestamp_prefix


def extract_df_to_csv(df_function, filename, fileloc="../data_raw/"):
    """
    Creates a raw csv extract from the chosen df returning function, with choice of filename, defaulting to the raw data location.
    Returns full file loc.
    """
    # extract_df_to_csv(sho.get_all_products_df(), 'shopify_allproducts')
    # extract_df_to_csv(sho.get_all_customers_df()(), 'shopify_allcustomers')
    # extract_df_to_csv(sho.get_all_orders_df(), 'shopify_allorders')
    # extract_df_to_csv(sho.get_product_variants_df(), 'shopify_allproductvariants')

    df = df_function

    prefix = get_timestamp_prefix()

    filefullpath = f"{fileloc}{prefix} {filename}.csv"

    df.to_csv(filefullpath, index=False)

    print(f"File created: {filefullpath}")
    return filefullpath


def load_to_bigquery(file_path, destination_loc, disposition="WRITE_TRUNCATE"):
    """
    Uploads a csv to a BigQuery table. Converts csv to a df and cleans column titles before loading.
    Destination loc should be in the format ".<dataset name>.<table name>", like ".raw.products_raw"
    Disposition dictates the load behavior. Default (and would be if unspecified) is WRITE_TRUNCATE, which overwrites.
        Use WRITE_APPEND to not overwrite and just append (e.g. incremental loads)
    """

    project = os.getenv("gcp_bigquery_project_name")

    client = bigquery.Client(project=project)

    df = pd.read_csv(file_path)

    # quick edit to test upload
    df.columns = df.columns.str.replace(".", "_", regex=False)

    job_config = bigquery.LoadJobConfig(write_disposition=disposition)

    client.load_table_from_dataframe(
        df, project + destination_loc, job_config=job_config
    ).result()
    print(f" Loaded {file_path} to BigQuery location {destination_loc}")


def allproducts_extract_upload():
    """Creates a csv of the all products extract and loads to BigQuery."""

    file = extract_df_to_csv(sho.get_all_products_df(), "shopify_allproducts")

    load_to_bigquery(file, ".raw.products_raw")

    print("done")


def allcustomers_extract_upload():
    """Creates a csv of the all customer extract and loads to BigQuery."""

    file = extract_df_to_csv(
        sho.get_all_customers_df(), "shopify_allcustomers"
    )

    load_to_bigquery(file, ".raw.customers_raw")

    print("done")


def allproductvariants_extract_upload():
    """Creates a csv of the all product variants extract and loads to BigQuery."""

    file = extract_df_to_csv(
        sho.get_product_variants_df(), "shopify_allproductvariants"
    )

    load_to_bigquery(file, ".raw.productvariants_raw")

    print("done")


def incrementalorders_extract_upload():
    """Creates a csv of the all orders extract and loads to BigQuery."""

    last_updated_dt = datetime(2025, 11, 1)

    file = extract_df_to_csv(
        sho.get_incremental_orders_df(last_updated_dt),
        "shopify_incrementalorders",
    )

    load_to_bigquery(file, ".raw.orders_raw", disposition="WRITE_APPEND")

    print("done")
