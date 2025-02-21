# NYC Taxi Azure Data Engineering Project
<img width="827" alt="image" src="https://github.com/user-attachments/assets/71b3cafa-eb63-4338-9f9f-e46586aad967" />


## Project Background
This project leverages **Azure Synapse Analytics** to analyze **NYC Green Taxi data** from 2020 to 2021. The goal is to provide actionable insights into taxi demand trends, payment preferences, and borough-level disparities. The project demonstrates the integration of Azure services, including **Serverless SQL Pool**, **Spark Pool**, and **Synapse Link**, to ingest, transform, and analyze large-scale data.

### Key Business Metrics
- **Dataset:** NYC Green Taxi trips (2020–2021)
- **Data Volume:** Millions of records, partitioned by year and month.
- **Business Goal:** Optimize fleet allocation, understand payment trends, and improve service strategies based on demand patterns.

---

## Data Structure & Initial Checks
The dataset consists of the following tables:

### Dimension Tables:
- **Taxi Zone:** CSV files (with and without headers)
- **Calendar:** CSV file
- **Vendor:** CSV files (with escaped characters and unquoted fields)
- **Rate Code:** TSV (Tab-Separated)
- **Trip Type:** JSON files
- **Payment Type:** JSON files (including arrays)

### Fact Table:
- **Trip Data Green:** Partitioned by Year and Month in CSV, Parquet, and Delta formats.

<img width="825" alt="image" src="https://github.com/user-attachments/assets/84fb7fde-6da8-4200-9cb7-e4420345739f" />


---

## Executive Summary
### Overview of Findings
This project provides key insights into NYC Green Taxi demand and payment trends:
1. **Weekly Demand Fluctuations:** Taxi demand varies significantly from week to week.
2. **Borough-Level Demand Disparities:** Manhattan dominates taxi demand, while other boroughs show lower but growing demand.
3. **Monthly Demand Trends:** A downward trend in taxi demand is observed from January 2020 to July 2021.
4. **Card Payment Adoption:** Card payments are increasing, but cash transactions remain significant.
5. **Geographic Payment Preferences:** Card payment adoption varies across boroughs.

**The Dashboards were created in AWS Quicksight.**
<img width="702" alt="image" src="https://github.com/user-attachments/assets/617d679d-6745-47e3-913a-321109bcc4bf" />

---

## Insights Deep Dive
### Category 1: Demand Analysis

<img width="821" alt="image" src="https://github.com/user-attachments/assets/a1b6b509-3298-47b9-a5e3-ae3b7503bb6f" />


#### Main Insight 1: **Weekly Demand Fluctuations**
- **Finding:** Taxi demand varies considerably from week to week.
- **Supporting Data:** Demand fluctuates throughout the weeks depicted in the chart. Specific data points are difficult to determine from the image.
- **Implication:** Understanding these fluctuations is essential for dynamic resource allocation.

#### Main Insight 2: **Borough-Level Demand Disparities**
- **Finding:** Manhattan is the dominant borough for taxi demand.
- **Supporting Data:** Manhattan accounts for a significantly larger share of trips than other boroughs. Precise percentages are difficult to determine from the image.
- **Implication:** This disparity necessitates borough-specific service strategies.

#### Main Insight 3: **Monthly Demand Trends**
- **Finding:** A general decline in taxi demand is noticeable from January 2020 to July 2021.
- **Supporting Data:** The chart shows a clear downward trend over the months shown. Specific trip counts are difficult to extract from the image.
- **Implication:** Further investigation into the causes of this trend is warranted.

---

### Category 2: Payment Trends Analysis

<img width="820" alt="image" src="https://github.com/user-attachments/assets/75c0c673-4213-4048-b0bf-58760ddfe95b" />


#### Main Insight 1: **Card Payment Adoption**
- **Finding:** Card payments are becoming more prevalent, but cash transactions remain a substantial portion of the total.
- **Supporting Data:** The "Payment Type By Month" chart indicates a general upward trend for card payments and a downward trend for cash payments. However, cash still represents a large portion of all transactions.
- **Implication:** The increasing use of card payments suggests a growing preference for digital transactions.

#### Main Insight 2: **Geographic Payment Preferences**
- **Finding:** Card payment adoption differs by borough.
- **Supporting Data:** Based on the "Payment Type By Borough" chart, Manhattan exhibits the highest proportion of card payments, followed by Brooklyn and Queens. Other boroughs seem to favor cash transactions.
- **Implication:** Targeted campaigns could potentially increase digital payment adoption.

---

## Recommendations
Based on the insights above, we recommend the following actions:
1. **Dynamic Resource Allocation:** Adjust fleet availability based on weekly demand fluctuations to optimize resource utilization.
2. **Borough-Specific Strategies:** Develop tailored service strategies for boroughs like Brooklyn and Queens to capture growing demand.
3. **Investigate Demand Decline:** Analyze external factors (e.g., pandemic impact, competition from ride-sharing apps) contributing to the downward trend in demand.
4. **Promote Card Payments:** Launch targeted campaigns in boroughs with lower card payment adoption to encourage digital transactions.
5. **Real-Time Monitoring:** Use Synapse Link and QuickSight dashboards to monitor trends and adjust strategies dynamically.

---

## Assumptions and Caveats
1. **Assumption 1:** Missing payment data for certain months was imputed using historical trends.
2. **Assumption 2:** Trips with unknown boroughs were excluded from the borough-level analysis.
3. **Assumption 3:** The data reflects only Green Taxi trips and may not represent the entire NYC taxi market.

---

## Services Used
- **Azure Synapse Analytics:** For data ingestion, transformation, and integration.
- **Serverless SQL Pool:** For initial data processing and querying.
- **Spark Pool:** For complex transformations and partition management.
- **Synapse Link:** For real-time data integration with Azure Cosmos DB.
- **AWS S3, Athena, and QuickSight:** For data storage, querying, and visualization.

---
### Solution Architecture
- **Overview:** The architecture combines **Serverless SQL Pool** and **Spark Pool** to optimize data ingestion, transformation, and analysis.
- **Key Components:**
  - **Bronze Layer:** Raw data ingestion.
  - **Silver Layer:** Cleaned and standardized data.
  - **Gold Layer:** Final, business-ready data for analysis.
    
- **Final Solution Architecture (using Spark)**
<img width="1003" alt="image" src="https://github.com/user-attachments/assets/13e066de-1ff2-4b7c-ab58-ea4f2f448a11" />

- **Initial Solution Architecture (using Serverless SQL)**
<img width="1001" alt="image" src="https://github.com/user-attachments/assets/38647d05-6e48-41c2-a5ea-cd1073ee6a80" />


---

## Project Execution Flow
1. **Discovery and Exploration:** Analyzed the dataset using T-SQL to understand its structure and quality.
2. **Data Ingestion (Bronze Schema):** Ingested raw data into Azure Synapse Analytics.
3. **Data Transformation (Silver Schema):** Cleaned and standardized data using Serverless SQL Pool and Spark Pool.
4. **Data Integration (Gold Schema):** Aggregated and finalized data for analysis.
5. **ETL Pipeline and Scheduling:** Automated data flow using Synapse pipelines.
6. **Data Visualization and Reporting:** Exported data to AWS QuickSight for interactive dashboards.

---


## Optional Reading: Pipeline Implementation Details

This section provides a deeper dive into the **pipelines** and **Apache Spark Pool** implementation used in this project. It is intended for readers who want to understand the technical aspects of the data transformation process.

---

### Pipelines Created in Synapse

#### 1. **pl_create_silver_tables**
- **Purpose:** Creates the silver tables for all non-partitioned data.
- **Functionality:** 
  - Uses the `For Each` function to capture metadata of files and stored procedures.
  - Deletes previous tables and recreates them based on updated metadata filenames and stored procedures.
  - Ensures **idempotency**, meaning the pipeline can be executed multiple times without changing the result beyond the initial application.
- **Key Feature:** Idempotency ensures consistency and reliability in data transformations.

<img width="1004" alt="image" src="https://github.com/user-attachments/assets/e01169fa-d755-49f6-8d04-cea055cbe87c" />

---

#### 2. **pl_create_silver_trip_data_green**
- **Purpose:** Creates the silver tables for partitioned data.
- **Functionality:**
  - Uses a script to extract partitions from the bronze view of the trip data file.
  - Deletes previous partitions and recreates them based on updated partitions from the bronze layer.
  - Creates a silver view that supports **partition pruning**, improving query performance by filtering unnecessary partitions.
- **Key Feature:** Partition pruning optimizes query performance for large datasets.

<img width="1004" alt="image" src="https://github.com/user-attachments/assets/4e343681-b687-427c-9c25-fb8d19704255" />

---

#### 3. **pl_create_gold_trip_data_green**
- **Purpose:** Creates the final gold view.
- **Functionality:**
  - Uses a script to extract partitions from the silver view.
  - Deletes previous partitions and recreates them based on updated partitions from the silver layer.
  - Joins dimension tables to the fact table and aggregates data to represent key business insights.
  - Supports **partition pruning** for efficient querying and filtering.
- **Key Feature:** The gold view provides a refined, business-ready dataset for analysis.

<img width="1004" alt="image" src="https://github.com/user-attachments/assets/d86a9c91-af04-490a-babf-4e0dcdceac20" />

---

#### 4. **The Master Pipeline**
- **Purpose:** Connects all pipelines, forms dependencies, and schedules automatic execution.
- **Functionality:**
  - Orchestrates the execution of `pl_create_silver_tables`, `pl_create_silver_trip_data_green`, and `pl_create_gold_trip_data_green`.
  - Uses a **Schedule Trigger** to automate pipeline execution.
- **Key Feature:** Ensures seamless and automated data transformation workflows.

<img width="1006" alt="image" src="https://github.com/user-attachments/assets/bff13013-29f3-4909-b422-502362645b1d" />

---

### Initial Limitation of Synapse Serverless SQL: 
- **Issue:** Serverless SQL does not maintain partitions during the transformation phase, which can lead to inefficiencies.
- **Workaround:** Dynamic datasets and stored procedures were used to recreate partition parquet files.
One stored procedure was created for each partition, ensuring partitions were maintained.
- **Scalability Challenge:** This approach does not scale efficiently for larger datasets.
- **Solution:** Transition to Apache Spark Pool for transformations.

### The Solution: Apache Spark Pool

#### Overview
- **Purpose:** Apache Spark Pool is designed for large-scale data processing and analytics.
- **Key Features:**
  - Highly scalable and distributed processing engine.
  - Supports complex data transformations, machine learning, and advanced analytics.
  - Seamlessly integrates with Azure Synapse Analytics.

#### Spark Pool Configuration
- **Requirements:** 
  - Minimum of one master node and two worker nodes.
  - Configurations tailored to the size and complexity of the dataset.

<img width="1006" alt="image" src="https://github.com/user-attachments/assets/b98448ae-f255-4645-95c8-ddc5b4acffa9" />

#### Spark Notebook Integration
- **Functionality:**
  - A Spark notebook transforms silver data into the final gold-tier aggregated format.
  - Utilizes existing partitions within the silver `trip_data_green` parquet files.
  - Eliminates the need for dynamic datasets or stored procedures.
- **Benefits:**
  - Faster and more scalable transformations.
  - Easier to maintain and debug compared to Serverless SQL pipelines.

<img width="1003" alt="image" src="https://github.com/user-attachments/assets/8c6817ea-b58d-4584-a286-9c46dd856080" />
<img width="1005" alt="image" src="https://github.com/user-attachments/assets/93fc5d3a-d38e-4abe-adb0-5fba2fe6e132" />

---

This optional reading section provides a detailed look into the technical implementation of the pipelines and Spark Pool. It is designed for readers who want to understand the inner workings of the data transformation process.
