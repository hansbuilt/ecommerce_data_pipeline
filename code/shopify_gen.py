import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import timedelta, datetime

load_dotenv()


def get_all_products_df():
    """Return a df of all products from the Shopify store. (REST API)"""

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


def get_product_variants_df():
    """Return a df of all product variants from the Shopify store via GraphQL API."""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    url = f"https://{store_name}.myshopify.com/admin/api/2025-10/graphql.json"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    query = """
    query getProducts($cursor: String) {
      products(first: 50, after: $cursor) {
        edges {
          cursor
          node {
            id
            title
            handle
            variants(first: 100) {
              edges {
                node {
                  id
                  title
                  sku
                  price
                  inventoryQuantity
                  barcode
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    all_variants = []
    cursor = None

    while True:
        variables = {"cursor": cursor}
        response = requests.post(
            url, headers=headers, json={"query": query, "variables": variables}
        )

        if response.status_code != 200:
            raise Exception(f"GraphQL query failed: {response.text}")

        data = response.json()
        products = data["data"]["products"]

        for product_edge in products["edges"]:
            product = product_edge["node"]
            for variant_edge in product["variants"]["edges"]:
                variant = variant_edge["node"]
                all_variants.append(
                    {
                        "product_id": product["id"],
                        "product_title": product["title"],
                        "product_handle": product["handle"],
                        "variant_id": variant["id"],
                        "variant_title": variant["title"],
                        "sku": variant["sku"],
                        "price": variant["price"],
                        "inventory_quantity": variant["inventoryQuantity"],
                        "barcode": variant["barcode"],
                    }
                )

        if products["pageInfo"]["hasNextPage"]:
            cursor = products["pageInfo"]["endCursor"]
        else:
            break

    df = pd.DataFrame(all_variants)
    return df


def get_all_orders_df():
    """Return a df of all orders from the Shopify store. (REST API)"""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = (
        f"https://{store_name}.myshopify.com/admin/api/2025-10/orders.json"
    )

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    params = {"limit": 250}

    response = requests.get(base_url, headers=headers, params=params)

    data = response.json()

    df = pd.json_normalize(data["orders"])

    # missing pagination for now

    return df


def get_incremental_orders_df(last_updated_dt, update_buffer=600):
    """Return a df of orders updated after the given date from the Shopify store. (REST API)
    last_update_dt passed as datetime object, like datetime(2025, 11, 1, 0, 0)
    Update_buffer helps pull back those n seconds before the latest updated date in case of timing issues.


    last_updated_dt = datetime(2025, 11, 1, 0, 0)
    get_incremental_orders_df(last_updated_dt)

    """

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = (
        f"https://{store_name}.myshopify.com/admin/api/2025-10/orders.json"
    )

    start_dt = (last_updated_dt - timedelta(seconds=update_buffer)).isoformat()

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    # set min update_at dt, sort by date for pagination's sake
    params = {
        "limit": 250,
        "status": "any",
        "updated_at_min": start_dt,
        "order": "updated_at asc",
    }

    all_orders = []
    url = base_url

    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        orders = data.get("orders", [])
        all_orders.extend(orders)

        link_header = response.headers.get("Link", "")

        next_url = None
        if 'rel="next"' in link_header:
            parts = link_header.split(",")
            for p in parts:
                if 'rel="next"' in p:
                    # extract the URL between < and >
                    next_url = p[p.find("<") + 1 : p.find(">")]
                    break

        if not next_url:
            break

        url = next_url
        params = None

    if not all_orders:
        return pd.DataFrame()

    df = pd.json_normalize(all_orders)
    return df


def get_all_customers_df():
    """Return a df of all customers from the Shopify store. (REST API)"""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = (
        f"https://{store_name}.myshopify.com/admin/api/2025-10/customers.json"
    )

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    params = {"limit": 250}

    response = requests.get(base_url, headers=headers, params=params)

    data = response.json()

    df = pd.json_normalize(data["customers"])

    # missing pagination for now

    return df
