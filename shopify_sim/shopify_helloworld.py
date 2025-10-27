import os

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_all_products_df():
    """Return a df of all products from the Shopify store."""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = (
        f"https://{store_name}.myshopify.com/admin/api/2025-10/products.json"
    )

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    params = {"limit": 250}

    response = requests.get(base_url, headers=headers, params=params)

    data = response.json()

    df = pd.json_normalize(data["products"])

    # missing pagination for now

    return df
