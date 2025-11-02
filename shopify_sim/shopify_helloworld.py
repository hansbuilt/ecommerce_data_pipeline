import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv

load_dotenv()

# https://shopify.dev/docs/api/admin-graphql#rate-limits


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


def create_order_narrowscope(customerId, lineItemList, addressDict):
    """Create a single order with limited scope for the purposes of mocking up orders.

    customerId - int, will get joined with "gid://shopify/Customer/" string
    lineItemList - a list of dicts, one list item per lineitem, each list item a dict of variantID and quantity
    addressDict - a dict of firstName, lastName, address1, city, province, country, zip; will use same address for billto and shipto

    example args:
        customerId = 9413291442458
        lineItemList = [
            {
                "variantId": "gid://shopify/ProductVariant/51245325123866",
                "quantity": 1,
            },
            {
                "variantId": "gid://shopify/ProductVariant/51245325680922",
                "quantity": 2,
            },
        ]
        addressDict = {
            "firstName": "Jane",
            "lastName": "Doe",
            "address1": "123 Main St",
            "city": "Milwaukee",
            "province": "WI",
            "country": "US",
            "zip": "53202",
        }


    """

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    base_url = (
        f"https://{store_name}.myshopify.com/admin/api/2025-01/graphql.json"
    )

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    mutation = """
        mutation CreateOrder($order: OrderCreateOrderInput!) {
          orderCreate(order: $order) {
            order {
              id
              name
              email
              createdAt
              shippingAddress {
                    firstName
                    lastName
                    address1
                    city
                    province
                    country
                    zip
                  }
                  billingAddress {
                    firstName
                    lastName
                    address1
                    city
                    province
                    country
                    zip
                  }
              totalPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """

    variables = {
        "order": {
            # "email": "customerr@example.com",
            # "customerId": "gid://shopify/Customer/9413291442458",
            "customerId": f"gid://shopify/Customer/{customerId}",
            "lineItems": lineItemList,
            "shippingAddress": addressDict,
            "billingAddress": addressDict,
            # "lineItems": [
            #     {
            #         "variantId": "gid://shopify/ProductVariant/51245325123866",
            #         "quantity": 1,
            #     },
            #     {
            #         "variantId": "gid://shopify/ProductVariant/51245325680922",
            #         "quantity": 2,
            #     },
            # ],
            # "shippingAddress": {
            #     "firstName": "Jane",
            #     "lastName": "Doe",
            #     "address1": "123 Main St",
            #     "city": "Milwaukee",
            #     "province": "WI",
            #     "country": "US",
            #     "zip": "53202",
            # },
            # "billingAddress": {
            #     "firstName": "Jane",
            #     "lastName": "Doe",
            #     "address1": "123 Main St",
            #     "city": "Milwaukee",
            #     "province": "WI",
            #     "country": "US",
            #     "zip": "53202",
            # },
            "financialStatus": "PAID",
        }
    }

    response = requests.post(
        base_url,
        headers=headers,
        json={"query": mutation, "variables": variables},
    )

    if response.status_code == 200:
        data = response.json()
        if "errors" in data:
            print("GraphQL errors:", json.dumps(data["errors"], indent=2))
        else:
            print(json.dumps(data, indent=2))
            print("Order created successfully")
    else:
        print(f"HTTP error {response.status_code}: {response.text}")


def create_customer(
    first_name,
    last_name,
    email,
    address1,
    city,
    province,
    country,
    zipcode,
    phone=None,
):
    """Create a single customer. (GraphQL)"""

    """
    first_name = 'testf'
    last_name = 'testl'
    email = 'test2@example.com'
    phone=None
    address1='123 Main St'
    city='Milwaukee'
    province='WI'
    country='US'
    zipcode='53202'

    create_customer('testf1', 'testl1', 'test11@test.com')
    """

    store_name = os.getenv("store_name")
    access_token = os.getenv("access_token")

    url = f"https://{store_name}.myshopify.com/admin/api/2025-10/graphql.json"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    query = """
    mutation createCustomer($input: CustomerInput!) {
      customerCreate(input: $input) {
        customer {
          id
          firstName
          lastName
          email
          phone
          createdAt
          addresses {
              id
              address1
              city
              province
              country
              zip
              }
        
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variables = {
        "input": {
            "firstName": first_name,
            "lastName": last_name,
            "email": email,
            "phone": phone,
            "addresses": [
                {
                    "address1": address1,
                    "city": city,
                    "province": province,
                    "country": country,
                    "zip": zipcode,
                }
            ],
        }
    }

    response = requests.post(
        url, headers=headers, json={"query": query, "variables": variables}
    )
    data = response.json()

    if response.status_code != 200:
        raise Exception(f"Request failed: {response.text}")

    if "errors" in data:
        errorlist = data["errors"]

        if len(errorlist) > 0:
            for e in errorlist:
                print(f"Error: {e['message']}")
            return None

    if "userErrors" in data["data"]["customerCreate"]:
        userErrorList = data["data"]["customerCreate"]["userErrors"]
        if len(userErrorList) > 0:
            for e in userErrorList:
                print(f"Error: {e['field']} || {e['message']}")
            return None

    customer = data["data"]["customerCreate"]["customer"]

    print(
        f"Created customer {customer['firstName']} {customer['lastName']} ({customer['email']})"
    )

    return customer


# customer creation function
# customer creation dataset - random name, addr, email, etc
# order building dataset - order count, random (weighted) selection of customer, line item count, variantIDs, quantities,
