import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_all_products_df():
    """Return a df of all products from the Shopify store. (REST API)"""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = f"https://{store_name}.myshopify.com/admin/api/2025-10/products.json"

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

    base_url = f"https://{store_name}.myshopify.com/admin/api/2025-10/orders.json"

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


def get_all_customers_df():
    """Return a df of all customers from the Shopify store. (REST API)"""

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = f"https://{store_name}.myshopify.com/admin/api/2025-10/customers.json"

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
