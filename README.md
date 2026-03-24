#  Customer Orders Data Pipeline (End-to-End Data Pipeline with Azure Databricks)

An Azure Databricks project that creates a data pipeline to generate data that can now be used for downstream users for their specific needs. It demonstrates my ability to **write production-quality Pyspark and SQL codes, design efficient queries, and build robust data pipelines so that users can draw insights from the data generated**.

The solution demonstrates best practices in data warehousing, transformation, using Databricks and Azure.

The pipeline processes customer orders, products, and their details through a medallion architecture (Bronze → Silver → Gold), implementing incremental loading, slowly changing dimensions (SCD Type 1 and 2) using Delta Live Tables DLT, to create analytics-ready datasets.

## 🏗️ Architecture

--insert image here

### Data Flow
```
Source Data (Parquet) → ADLS Gen 2 → Databricks (Staging) → Bronze Layer → Silver Layer → Gold Layer
                                                                ↓              ↓           ↓
                                                              Raw Data    Cleaned Data    Analytics
```
### Technology Stack

- **Cloud Data Warehouse**: Azure Databricks
- **Transformation Layer**: Azure Databricks
- **Cloud Storage**: ADLS Gen2
- **Version Control**: Git
- **Key Azure Databricks Features**:
  - AutoLoader for Incremental Loading
  - Dbutils Widgets for Parameterization
  - Delta Live Tables (SCD Type 2)
  - Databricks Workflows
  - Adls Gen2 Hierachical Namespace(Delta Lake)
  - Unity Catalog
 
## 📊 Data Model

### Medallion Architecture

#### 🥉 Bronze Layer (Raw Data)
Raw data ingested from Delta Lake:
- `products` - Raw products information 
- `customers` - Raw customer details
- `orders` - Raw customer orders
- `regions` - Raw customer locations(static_file)

#### 🥈 Silver Layer (Cleaned Data)
Cleaned and standardized data:
- `silver_products` - Validated and Standardized Product Details
- `silver_customers` - Validated and Standardized Customer Information.
- `silver_orders` - Validated Customer orders

#### 🥇 Gold Layer (Analytics-Ready)
Business-ready datasets optimized for analytics:
- `gold_customers` - Dimension table for customer details and implementing SCD Type 1 on it.
- `gold_products` - Dimension table for product information, implemented via DLT with SCD Type 2 to track historical product changes.
- `gold_orders` - Denormalized fact table joining customers and products.

## 📁 Project Structure

```
dbx_project/
├── README.md                           # This file
├── parameters                          # parameters used inside the data ingestion script
├── bronze_layer.ipynb                  # data ingestion scripts
├── silver_customers.ipynb              # transformation logic for customer details
├── silver_products.ipynb               # transformation logic for product details
├── silver_orders.ipynb                 # tramsformation logic for order details
├── gold_customer.ipynb                 # fact table for customer details
├── gold_orders.ipynb                   # dimension table for customer orders
│
├── transformations/                         # DLT folder
│   ├── Gold_products_p.py
│   
```
## 🚀 Getting Started

### Prerequisites

**Azure Cloud Services (will create one if doesn't exist)**
   - For Delta Lake(ADLS Gen 2) via Storage account
   - Resource Groups
   - Azure Databricks
   - Access Connector


## 🎯 Key Features

### 1. Incremental Loading
Data is ingested via Spark Structured Streaming/Autoloader to process only new/changed data:
```sql
df = spark.readStream.format('cloudFiles')\
                .option('cloudFiles.format', 'parquet')\
                .option('cloudFiles.schemaLocation', f'abfss://bronze@etedbxcrop.dfs.core.windows.net/checkpoint_{filename}')\
                .load(f'abfss://source@etedbxcrop.dfs.core.windows.net/{filename}')

df.writeStream.format('parquet')\
    .outputMode('append')\
    .option('checkpointlocation', f'abfss://bronze@etedbxcrop.dfs.core.windows.net/checkpoint_{filename}')\
    .option('path',f'abfss://bronze@etedbxcrop.dfs.core.windows.net/{filename}')\
    .trigger(once = True)\
    .start()
```

### 2. DbUtils Widgets
The dbutils.widgets utility in Databricks provides a way to create interactive input parameters in notebooks and dashboards without editing the underlying code. These widgets allow users to select values or input text, which can then be retrieved and used in the notebook's logic, enhancing interactivity and reusability. 
  ```
dbutils.widgets.text('filename', 'products')
filename = dbutils.widgets.get('filename')
print(filename)
  ```

### 3. Delta Live Tables
Delta Live Tables (DLT) in Databricks is a declarative ETL framework that simplifies building, managing, and monitoring reliable batch and streaming data pipelines. It enables developers to define data transformations using Python or SQL, while the system automatically manages task orchestration, cluster resources, error handling, and data quality checks via "expectations". Its concerned with whats to be done, rather than how to do it.
```sql
import dlt
from pyspark.sql.functions import *

rules = {
    'rule1' : 'product_id is not null',
    'rule2' : 'product_name is not null'
}

@dlt.table()
@dlt.expect_all(rules)
def products_staging():
    df = spark.readStream.table('dbx_cata.silver.products')
    df = df.withColumn('updated_date', to_date(current_date()))
    return df
    ...
```

### 4. Databricks Workflows
Databricks Workflows (also known as Lakeflow Jobs) is a fully managed, native orchestration service within the Databricks Data Intelligence Platform. It enables users to build, schedule, and monitor complex ETL, analytics, and machine learning pipelines, supporting up to 1,000 tasks per job using notebooks, SQL, Python, or JAR

--insert image here

### 5. Unity Catalog
Databricks Unity Catalog is a unified data and AI governance solution built directly into the Databricks Lakehouse Platform. It provides a central place to manage data access, audit controls, and data discovery across multiple workspaces and clouds, using a standard ANSI SQL interface

## 📈 Key Learnings

- Designed scalable ETL pipelines using PySpark that can process large datasets efficiently
- Deployed pipeline on cloud (Azure)
- Parametrized notebooks to avoid rewriting same code over and over
- Used DLT to implement SCD Type 2
- Implemented best practices for data engineering workflows
- Integrated workspace with Git
- Added a validation layer using **'expectations'**

## 🔮 Future Improvements
- Integrate Airflow for more customizable orchestration.
- Integrate Databricks Asset Bundles for more structure and continuous integration/continuous deployment (CI/CD).
- Add more complex business metrics.
- Integrate with BI tools (Tableau/Power BI).
- Add alerting and monitoring.

## 🤝 Contributing

Contributions are welcome! Feel free to fork and submit a PR.

- Fork the repository
- Create a feature branch (git checkout -b feature/AmazingFeature)
- Commit your changes (git commit -m 'Add some AmazingFeature')
- Push to the branch (git push origin feature/AmazingFeature)
- Open a Pull Request
