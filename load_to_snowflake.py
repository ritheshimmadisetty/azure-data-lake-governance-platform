import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="RITHESH162",
    password="R@datadream6989",
    account="MXNVDOB-RI70382",  # e.g. abc12345.central-india.azure
    warehouse="DE_WH",
    database="DE_PROJECT",
    schema="SALES"
)

cursor = conn.cursor()

# Create raw tables
cursor.execute("""
    CREATE OR REPLACE TABLE RAW_CUSTOMERS (
        customer_id    INTEGER,
        customer_name  VARCHAR,
        city           VARCHAR,
        segment        VARCHAR,
        created_date   VARCHAR
    )
""")

cursor.execute("""
    CREATE OR REPLACE TABLE RAW_PRODUCTS (
        product_id    INTEGER,
        product_name  VARCHAR,
        category      VARCHAR,
        unit_price    FLOAT
    )
""")

cursor.execute("""
    CREATE OR REPLACE TABLE RAW_ORDERS (
        order_id     INTEGER,
        customer_id  INTEGER,
        product_id   INTEGER,
        quantity     INTEGER,
        order_date   VARCHAR,
        status       VARCHAR
    )
""")

# Load CSVs into Snowflake
customers_df = pd.read_csv("customers.csv")
products_df  = pd.read_csv("products.csv")
orders_df    = pd.read_csv("orders.csv")

customers_df.columns = [c.upper() for c in customers_df.columns]
products_df.columns  = [c.upper() for c in products_df.columns]
orders_df.columns    = [c.upper() for c in orders_df.columns]

write_pandas(conn, customers_df, "RAW_CUSTOMERS", quote_identifiers=False)
write_pandas(conn, products_df,  "RAW_PRODUCTS",  quote_identifiers=False)
write_pandas(conn, orders_df,    "RAW_ORDERS",    quote_identifiers=False)

print("✅ Raw data loaded into Snowflake successfully!")
print(f"Customers: {len(customers_df)} rows")
print(f"Products:  {len(products_df)} rows")
print(f"Orders:    {len(orders_df)} rows")

cursor.close()
conn.close()