# Azure Data Lake & Governance Platform

A data engineering project I built to practice working with the modern data stack end to end — from raw files landing in a data lake all the way to a queryable Snowflake warehouse, orchestrated automatically with Airflow.

---

## What this project does

Takes raw retail sales data (customers, products, orders) from multiple source formats and runs it through a full pipeline:

- Raw files land in **Azure Data Lake (ADLS Gen2)** via ADF pipelines
- **Databricks + PySpark** cleans and transforms the data through Bronze → Silver → Gold layers
- **dbt** builds a Star Schema in Snowflake with dimension tables, a fact table, and automated data quality tests
- **Apache Airflow** (running in Docker) orchestrates everything on a daily schedule with validation checks at each step

---

## Tech Stack

- **Cloud:** Azure (Data Factory, ADLS Gen2, Databricks, Synapse, Purview)
- **Processing:** Apache Spark, PySpark, Delta Lake
- **Warehouse:** Snowflake
- **Transformation:** dbt
- **Orchestration:** Apache Airflow, Docker
- **Data Modeling:** Star Schema, SCD Type 2
- **DevOps:** Git, GitHub Actions, Azure DevOps, CI/CD

---

## Pipeline

```
CSV / REST API / SQL Server
         │
         ▼
 Azure Data Factory        ← metadata-driven, picks up from all sources
         │
         ▼
  Bronze (ADLS Gen2)       ← raw data, nothing changed
         │
         ▼
  Databricks + PySpark     ← clean nulls, fix types, standardize
         │
         ▼
  Silver (Delta Lake)      ← cleaned, validated
         │
         ▼
  Databricks + PySpark     ← join tables, calculate totals, partition
         │
         ▼
  Gold (Delta Lake)        ← business-ready, partitioned by year/month
         │
         ▼
  dbt on Snowflake         ← Star Schema, SCD Type 2
         │
         ▼
  Snowflake DWH            ← fact_sales, dim_customer, dim_product, dim_date
```

Airflow runs this entire thing every day at 2am. If any step fails, downstream tasks don't run and the pipeline retries automatically.

---

## Data Model

```
              dim_date
                 │
dim_customer ── fact_sales ── dim_product
```

- `fact_sales` — one row per order, 1000 records
- `dim_customer` — 200 customers, SCD Type 2 (tracks city/segment changes over time)
- `dim_product` — 50 products across 4 categories
- `dim_date` — full date dimension 2023–2025

**Why SCD Type 2 on customers?** If a customer moves from Bengaluru to Mumbai, just overwriting the record would break historical reporting. SCD Type 2 keeps both versions with `effective_date`, `end_date`, and `is_current` — so you can always answer "what city was this customer in when they placed this order?"

---

## Airflow DAG

Six tasks, run in sequence:

```
validate_source_data → load_raw_to_snowflake → validate_snowflake_load
                                                        │
                                              run_dbt_models → run_dbt_tests → pipeline_complete
```

The validation steps act as gates — if row counts are wrong after loading, dbt never runs on bad data. Each task retries once on failure with a 5 minute wait.

---

## Project structure

```
de-project/
├── generate_data.py            # generates sample retail data
├── load_to_snowflake.py        # loads CSVs into Snowflake raw tables
├── customers.csv
├── products.csv
├── orders.csv
│
├── sales_dw/                   # dbt project
│   ├── models/
│   │   ├── staging/            # clean + rename from raw tables
│   │   ├── dimensions/         # dim_customer (SCD2), dim_product, dim_date
│   │   └── facts/              # fact_sales with dbt tests
│   └── dbt_project.yml
│
└── airflow-docker/
    ├── docker-compose.yml
    ├── .env.example
    └── dags/
        └── sales_pipeline.py
```

---

## How to run it

**Requirements:** Docker Desktop, a Snowflake account, an Azure account

```bash
# 1. Clone
git clone https://github.com/ritheshimmadisetty/azure-data-lake-governance-platform.git
cd azure-data-lake-governance-platform

# 2. Set up credentials
cp airflow-docker/.env.example airflow-docker/.env
# fill in your Snowflake details in .env

# 3. Generate sample data
python generate_data.py

# 4. Start Airflow
cd airflow-docker
docker compose up airflow-init
docker compose up -d

# 5. Open http://localhost:8080 (admin / admin123)
# Trigger the sales_data_pipeline DAG
```

**Check results in Snowflake:**

```sql
SELECT
    c.city,
    p.category,
    SUM(f.total_amount)  AS total_revenue,
    COUNT(f.order_id)    AS total_orders
FROM FACT_SALES f
JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
JOIN DIM_PRODUCT  p ON f.product_id  = p.product_id
WHERE f.status = 'Completed'
GROUP BY c.city, p.category
ORDER BY total_revenue DESC;
```

---

## A few decisions worth explaining

**Delta Lake over plain Parquet** — Delta gives ACID transactions. If a Spark job crashes halfway through writing, you don't end up with a half-written table. Plain Parquet doesn't have that guarantee.

**dbt for SQL transformations** — It's just SQL but with testing, version control, and lineage built in. Every model is documented and every column can be tested for nulls, uniqueness, or custom rules. Running `dbt test` before any downstream process catches data issues early.

**Docker for Airflow** — Avoids the classic "works on my machine" problem. The whole Airflow stack (webserver, scheduler, postgres) comes up with one command and behaves the same everywhere.

**Credentials in .env, never in code** — Learned this the hard way. All secrets go in `.env` which is gitignored. The code only reads `os.environ.get('SNOWFLAKE_PASSWORD')`.

---

## Author

Rithesh Immadisetty — Data Engineer, Bengaluru
[LinkedIn](https://linkedin.com/in/ritheshimmadisetty)
