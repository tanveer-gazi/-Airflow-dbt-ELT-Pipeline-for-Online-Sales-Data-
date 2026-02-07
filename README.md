# Airflow + dbt ELT Pipeline for Online Sales Data  
📅 Project Date: December 26, 2024  

## Overview  

This project showcases an end-to-end ELT (Extract, Load, Transform) data pipeline built using a modern data engineering stack.  

The pipeline automates ingestion of online sales data, loads it into a PostgreSQL data warehouse, applies transformations using dbt, and makes analytics-ready tables available for business intelligence and reporting through Apache Superset.  
The goal of this project is to simulate a production-style data platform commonly used in real-world analytics teams.

![ELT Pipeline Architecture](https://github.com/tanveer-gazi/-Airflow-dbt-ELT-Pipeline-for-Online-Sales-Data-/blob/47d3b336ec774ec7d35958e0be07533ab109052f/explanation%20snapshot.png)

## Data Source  

Online Sales Dataset (CSV format) obtained from Kaggle.  
The dataset represents transactional e-commerce sales data and is used to simulate daily data ingestion.

---

## Architecture  

**Data Flow:**  

CSV / API Source  
→ Python Ingestion Scripts  
→ PostgreSQL (Staging Layer)  
→ dbt Transformations (Analytics Layer)  
→ Apache Superset Dashboards  

**Core Components:**  

- Apache Airflow for orchestration and scheduling  
- PostgreSQL as the data warehouse  
- dbt for transformation and modeling  
- Docker Compose for containerized deployment  
- Apache Superset for visualization  

---

## Pipeline Workflow  

1. **Ingestion Layer**  
   - Python scripts extract raw sales data from CSV files (can be extended to API sources)  
   - Data is loaded into PostgreSQL staging tables  

2. **Orchestration**  
   - Airflow DAG automates the full ELT process  
   - Handles scheduling, dependencies, and retries  

3. **Transformation Layer**  
   - dbt models clean, transform, and aggregate raw data  
   - Creates analytics-ready fact and dimension tables  

4. **Analytics & Reporting**  
   - Superset connects to PostgreSQL  
   - Dashboards provide insights into sales performance  

---

## Tech Stack  

- Python (Pandas, psycopg2)  
- Apache Airflow  
- PostgreSQL  
- dbt  
- Docker & Docker Compose  
- Apache Superset  

---

## Key Features  

- Fully automated ELT workflow  
- Modular pipeline design  
- dbt tests for data quality  
- Containerized environment for easy deployment  
- Scalable architecture similar to production systems  

---

## Running the Project  

### Prerequisites  

- Docker  
- Docker Compose  

### Steps  

1. Clone the repository  
run the following command:

git clone <your-github-repo-url>
cd airflow-dbt-elt-pipeline

2. Start all services 
run the following command:
docker-compose up --build

3. Access services
run the following command:
Airflow UI → http://localhost:8080
Superset UI → http://localhost:8088
PostgreSQL → localhost:5432
