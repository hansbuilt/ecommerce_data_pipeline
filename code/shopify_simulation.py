import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv
import random
from faker import Faker
from datetime import datetime, timedelta
import time
import shopify_gen as sho

load_dotenv()

# https://shopify.dev/docs/api/admin-graphql#rate-limits


def create_order_narrowscope(customerId, lineItemList, addressDict, processedAt=None):
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

    base_url = f"https://{store_name}.myshopify.com/admin/api/2025-01/graphql.json"

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
              processedAt
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
            # "processedAt": processedAt,
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

    # if processedAt is included, added it to the payload
    if processedAt:
        variables["order"]["processedAt"] = processedAt

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


def random_number_exp(min_qty, max_qty, weight):
    """Return a random order size biased toward small values, using an exponential distribution."""
    # ^2 = moderate bias, ^3 = stronger
    r = random.random() ** weight
    return int(min_qty + (max_qty - min_qty) * r)


def get_random_citystatezip():
    """
    Returns the city, state, zipcode values of a random location in the USA, weighted by population.

    Uses dataset from here: https://www.kaggle.com/datasets/bwandowando/us-zip-codes-database-from-simplemaps-com?resource=download

    """

    dataloc = "../data_supp/uszips.csv"

    # ensure leading zeros on zip are retained
    df = pd.read_csv(dataloc, dtype={"zip": str})

    randloc = df.sample(n=1, weights="population", random_state=None)

    city, state, zipcode = randloc.iloc[0][["city", "state_id", "zip"]]

    return city, state, zipcode


def get_fake_nameaddressemail_dict():
    """
    Returns a dict of firstName, lastName, address1, city, province, country (US is hardcoded), and zip.

    """

    fake = Faker()

    # get fake random first and last name
    firstName = fake.first_name()
    lastName = fake.last_name()
    email = fake.email()

    # get fake random address from Faker, but just keep the first address line
    address = fake.address()
    address1 = address.split("\n")[0]

    # get a legit city/state/zip
    city, state, zipcode = get_random_citystatezip()

    addr = {
        "firstName": firstName,
        "lastName": lastName,
        "address1": address1,
        "city": city,
        "province": state,
        "country": "US",
        "zip": zipcode,
        "email": email,
    }

    print(addr)

    return addr


def customer_single_generator():
    d = get_fake_nameaddressemail_dict()

    create_customer(
        d["firstName"],
        d["lastName"],
        d["email"],
        d["address1"],
        d["city"],
        d["province"],
        d["country"],
        d["zip"],
    )


def pick_random_date_last_24_months():
    """Pick a random date within the last 24 months."""

    now = datetime.utcnow()
    start = now - timedelta(days=365 * 2)
    delta_seconds = int((now - start).total_seconds())
    random_seconds = random.randint(0, delta_seconds)
    random_date = start + timedelta(seconds=random_seconds)

    return random_date.isoformat() + "Z"


def order_single_generator(randDate=False):
    """Generate a single random order."""

    # gather random single existing customer (need name, address, email)
    dfcust = sho.get_all_customers_df().sample(n=1)

    customerId = int(dfcust["id"].values[0])

    # build cust address dict for order
    addressDict = {
        "firstName": dfcust["default_address.first_name"].values[0],
        "lastName": dfcust["default_address.last_name"].values[0],
        "address1": dfcust["default_address.address1"].values[0],
        "city": dfcust["default_address.city"].values[0],
        "province": dfcust["default_address.province"].values[0],
        "country": dfcust["default_address.country"].values[0],
        "zip": dfcust["default_address.zip"].values[0],
    }

    # gather all existing product variants (need the ids)
    dfvars = sho.get_product_variants_df()

    # get a list of variant IDs to select from
    variants = dfvars["variant_id"].tolist()

    # get the randomly selected count of line items for the order
    lineCount = random_number_exp(1, 5, 3)

    lineItemList = []
    for i in range(0, lineCount):
        # randomly select a variant, randomly choose the number ordered
        variant = random.choice(variants)
        quantity = random_number_exp(1, 3, 3)

        lineItemList.append({"variantId": variant, "quantity": quantity})

    # randomly select a date, if option picked, else it'll be the current time
    if randDate:
        processedAt = pick_random_date_last_24_months()
    else:
        processedAt = None

    create_order_narrowscope(customerId, lineItemList, addressDict, processedAt)

    print("done")


def create_multiple_customers(customerCount):
    """Generate a specified number of customers."""

    for i in range(0, customerCount):
        try:
            customer_single_generator()
        except:
            break
        finally:
            print(f"Created customer {i+1} of {customerCount}")


def create_multiple_orders(orderCount):
    """Generate a specified number of orders. Per Shopify documentation, only 5 allowed per minute on dev stores."""

    for i in range(0, orderCount):
        try:
            order_single_generator(randDate=True)
        except:
            break
        finally:
            print(f"Created order {i+1} of {orderCount}")
            print("Sleeping 13 seconds...")
            time.sleep(13)
