import shopify_gen as sho
import pandas as pd
from datetime import datetime


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
