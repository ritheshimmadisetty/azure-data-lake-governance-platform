from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'rithesh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='sales_data_pipeline',
    default_args=default_args,
    description='End-to-end sales pipeline: validate → load → dbt → test',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'data-engineering', 'project']
)

# Inside Docker, files are mounted at these paths
DATA_PATH    = '/opt/airflow/data'
DBT_PATH     = '/opt/airflow/sales_dw'
SNOWFLAKE_CONFIG = {
    'user'     : os.environ.get('SNOWFLAKE_USER'),
    'password' : os.environ.get('SNOWFLAKE_PASSWORD'),
    'account'  : os.environ.get('SNOWFLAKE_ACCOUNT'),
    'warehouse': 'DE_WH',
    'database' : 'DE_PROJECT',
    'schema'   : 'SALES'
}

def task_validate_source_data():
    import pandas as pd
    files = ['customers.csv', 'products.csv', 'orders.csv']
    for f in files:
        path = os.path.join(DATA_PATH, f)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing: {path}")
        df = pd.read_csv(path)
        if len(df) == 0:
            raise ValueError(f"Empty file: {f}")
        print(f"✅ {f} — {len(df)} rows")
    print("✅ All source files validated")

def task_load_to_snowflake():
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    cur.execute("CREATE OR REPLACE TABLE RAW_CUSTOMERS (customer_id INTEGER, customer_name VARCHAR, city VARCHAR, segment VARCHAR, created_date VARCHAR)")
    cur.execute("CREATE OR REPLACE TABLE RAW_PRODUCTS  (product_id INTEGER, product_name VARCHAR, category VARCHAR, unit_price FLOAT)")
    cur.execute("CREATE OR REPLACE TABLE RAW_ORDERS    (order_id INTEGER, customer_id INTEGER, product_id INTEGER, quantity INTEGER, order_date VARCHAR, status VARCHAR)")

    for fname, table in [("customers.csv","RAW_CUSTOMERS"),("products.csv","RAW_PRODUCTS"),("orders.csv","RAW_ORDERS")]:
        df = pd.read_csv(os.path.join(DATA_PATH, fname))
        df.columns = [c.upper() for c in df.columns]
        write_pandas(conn, df, table, quote_identifiers=False)
        print(f"✅ Loaded {len(df)} rows into {table}")

    cur.close()
    conn.close()

def task_validate_snowflake_load():
    import snowflake.connector
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    for table, minimum in [("RAW_CUSTOMERS",200),("RAW_PRODUCTS",50),("RAW_ORDERS",1000)]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        if count < minimum:
            raise ValueError(f"❌ {table} has {count} rows — expected {minimum}")
        print(f"✅ {table}: {count} rows")

    cur.close()
    conn.close()

def task_run_dbt():
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", DBT_PATH],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("❌ dbt run failed")
    print("✅ dbt run complete")

def task_run_dbt_tests():
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", DBT_PATH],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("❌ dbt tests failed")
    print("✅ dbt tests passed")

def task_pipeline_complete():
    import snowflake.connector
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*), SUM(total_amount) FROM FACT_SALES WHERE status = 'Completed'")
    count, revenue = cur.fetchone()

    cur.close()
    conn.close()

    print("=" * 50)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print(f"   Orders   : {count}")
    print(f"   Revenue  : {revenue:,.2f}")
    print(f"   Timestamp: {datetime.now()}")
    print("=" * 50)

# Define tasks
t1 = PythonOperator(task_id='validate_source_data',    python_callable=task_validate_source_data,    dag=dag)
t2 = PythonOperator(task_id='load_raw_to_snowflake',   python_callable=task_load_to_snowflake,       dag=dag)
t3 = PythonOperator(task_id='validate_snowflake_load', python_callable=task_validate_snowflake_load, dag=dag)
t4 = PythonOperator(task_id='run_dbt_models',          python_callable=task_run_dbt,                 dag=dag)
t5 = PythonOperator(task_id='run_dbt_tests',           python_callable=task_run_dbt_tests,           dag=dag)
t6 = PythonOperator(task_id='pipeline_complete',       python_callable=task_pipeline_complete,       dag=dag)

# Task order
t1 >> t2 >> t3 >> t4 >> t5 >> t6
