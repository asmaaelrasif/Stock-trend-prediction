# **Stock Trend Prediction Pipeline**

A data engineering pipeline that processes historical stock data to identify price trends. The system ingests Apple stock data, performs feature engineering using distributed computing, and provides analytics through an interactive dashboard.

## **1\. Architecture Overview (Medallion Structure)**

The pipeline follows a standardized Medallion architecture, focusing on incremental and idempotent processing across different data quality stages:

1. **Data Ingestion (Bronze):** Downloads raw Apple stock data from a source (e.g., GitHub).  
2. **Raw Storage (Bronze):** Breaks data into monthly chunks and uploads the raw files to the **MinIO** data lake. MD5 checksums are used for integrity checks.  
3. **Transformation (Silver/Gold):** **Apache Spark** reads the raw files, calculates sophisticated features (like rolling averages and volatility), and enforces schema quality.  
4. **Feature Store (Gold):** **PostgreSQL** stores the final analytics-ready data, serving as the data warehouse.  
5. **Visualization:** **Metabase** connects to the PostgreSQL warehouse for interactive dashboards and business intelligence.

The pipeline is orchestrated by **Apache Airflow** to manage dependencies and scheduling.

## **2\. Tech Stack**

The entire system is containerized using **Docker & Docker Compose** for easy, single-command deployment.

| Component | Technology | Role in Pipeline |
| :---- | :---- | :---- |
| **Orchestration** | Apache Airflow | Schedules, manages dependencies, and tracks status of all jobs. |
| **Data Lake** | MinIO | S3-compatible object storage for raw and intermediate data files. |
| **Processing** | Apache Spark | Distributed processing engine for scalable feature calculation and transformation. |
| **Data Warehouse** | PostgreSQL | Stores the final, feature-engineered data for persistent access and querying. |
| **Visualization** | Metabase | Provides the interactive dashboard and business intelligence interface. |
| **Deployment** | Docker & Docker Compose | Containerization and service orchestration. |

## **3\. Features Calculated**

The pipeline's primary goal is to identify trend directions (Up, Down, Neutral) rather than predicting exact prices.



| Feature Name | Category | Calculation Basis | Purpose |
|:------------|:---------|:------------------|:--------|
| `daily_return` |  Momentum | Percentage change in closing price | Fundamental measure of daily performance |
| `ma_10d` |  Trend | 10-day Simple Moving Average | Identify short-term price trends and crossover signals |
| `ma_20d` |  Trend | 20-day Simple Moving Average | Identify medium-term price trends and crossover signals |
| `volatility_10d` | Risk | 10-day rolling standard deviation of returns | Quantify short-term market risk |
| `trend_label` |  Classification | Categorized based on `daily_return` vs. threshold | **Target variable** for predictive analysis |


**Trend Label Classification Logic:** The classification uses a simple $\\pm 0.1\\%$ threshold on the daily return:

* **Uptrend:** $\\texttt{daily\\\_return} \> 0.1\\%$  
* **Downtrend:** $\\texttt{daily\\\_return} \< \-0.1\\%$  
* **Neutral:** $-0.1\\% \\le \\texttt{daily\\\_return} \\le 0.1\\%$

## **4\. Conceptual Setup Instructions**

Make sure you have Docker and Docker Compose installed on your system.

### **A. Deployment**

1. Clone the repository:

git clone \[https://github.com/asmaaelrasif/Stock-trend-prediction.git\](https://github.com/asmaaelrasif/Stock-trend-prediction.git)  
cd Stock-trend-prediction

2. **Configure Environment Variables:** Ensure the file named .env exists in the root directory. This file contains all necessary ports and default credentials for the services to communicate and is mandatory before running the containers. **Note:** Modify credentials in this file if you plan to use this setup in a production environment.  
3. Build and start all services:

docker-compose up \-d \--build

4. Initialize Airflow (Creates users, connections, and sets up the environment):

docker compose up airflow-init

5. Verify all containers are running:

docker ps

6. Access the services:  
* **Airflow:** http://localhost:8082 (Orchestration)  
* **pgAdmin:** http://localhost:5050/ (PostgreSQL GUI)  
* **MinIO:** http://localhost:9001 (Data Lake UI)  
* **Metabase:** http://localhost:3000 (Dashboard)

### **B. How to Use**

1. Open the Airflow web interface.  
2. Enable and manually trigger the stock\_prediction\_dag.  
3. Monitor the pipeline execution (Ingest $\\rightarrow$ Chunk $\\rightarrow$ Spark Transform $\\rightarrow$ Load to Postgres).  
4. Once processing completes, access the Metabase dashboard to view trend analytics.

## **5\. Pipeline Details and Limitations**

### **Incremental Processing**

The pipeline runs daily and only processes new data chunks. It uses a **PostgreSQL tracking table** to remember which months have already been processed and relies on **MD5 checksums** to detect file changes. Crucially, the Spark job fetches the last 20 days of data from the previous period to ensure accurate rolling metric calculations (like $\\texttt{ma\\\_10d}$) at the start of a new monthly chunk, preventing cold-start issues.

### **Database Schema**

The final stock\_data table in PostgreSQL includes:

* date, open, high, low, close, volume  
* ticker, name  
* prev\_close, daily\_return  
* trend\_label  
* ma10, ma20  
* volatility\_10d  
* source\_file (for data lineage)

### **Known Limitations**

1. Currently optimized for single-ticker processing (Apple stock only).  
2. Fixed 20-day lookback window is hardcoded in the Spark script.  
3. Uses simple append mode for PostgreSQL loads (lacks upsert/merge logic).  
4. Designed for batch processing (no real-time streaming capability).



---
## **6\. Project Structure**

The root directory (Stock-trend-prediction/) contains the following structure:

<table>
<thead>
<tr>
<th width="25%">Path</th>
<th width="15%">Type</th>
<th width="60%">Description</th>
</tr>
</thead>
<tbody>

<!-- Directories -->
<tr>
<td><code>config/</code></td>
<td> <strong>Directory</strong></td>
<td>Configuration files and settings for services (e.g., Spark config)</td>
</tr>

<tr>
<td><code>dags/</code></td>
<td> <strong>Directory</strong></td>
<td>Apache Airflow DAG definitions</td>
</tr>

<tr>
<td><code>data/</code></td>
<td> <strong>Directory</strong></td>
<td>Local storage for raw data files</td>
</tr>

<tr>
<td><code>logs/</code></td>
<td> <strong>Directory</strong></td>
<td>Log output for Airflow and other services</td>
</tr>

<tr>
<td><code>plugins/</code></td>
<td> <strong>Directory</strong></td>
<td>Airflow custom operators, hooks, and extensions</td>
</tr>

<tr>
<td><code>scripts/</code></td>
<td> <strong>Directory</strong></td>
<td>General utility scripts (e.g., MinIO bucket setup)</td>
</tr>

<tr>
<td><code>spark-scripts/</code></td>
<td> <strong>Directory</strong></td>
<td>PySpark transformation scripts (feature engineering)</td>
</tr>

<!-- Files -->
<tr>
<td><code>Dockerfile.airflow</code></td>
<td> <strong>Docker</strong></td>
<td>Dockerfile for building the custom Airflow image</td>
</tr>

<tr>
<td><code>Dockerfile.spark</code></td>
<td> <strong>Docker</strong></td>
<td>Dockerfile for building the custom Spark image</td>
</tr>

<tr>
<td><code>docker-compose.yml</code></td>
<td> <strong>Compose</strong></td>
<td>Service orchestration for all components</td>
</tr>

<tr>
<td><code>init.sql</code></td>
<td> <strong>SQL</strong></td>
<td>SQL script to initialize PostgreSQL schemas/tables</td>
</tr>

<tr>
<td><code>.env</code></td>
<td> <strong>Config</strong></td>
<td>Environment variables and secrets <em>(mandatory)</em></td>
</tr>

</tbody>
</table>

---



## **7\. Authors**

* Mohammed Darwish  
* Asmaa Elrasef

DSAI 5102 \- Data Architecture & Engineering  
Fall 2025
