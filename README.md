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

## Solution Architecture
<img width="828" alt="image" src="https://github.com/user-attachments/assets/3726c80e-4564-43b1-b2a9-b5bfabcfb532" />


---

## Project Execution Flow
1. **Discovery and Exploration:** Analyzed the dataset using T-SQL to understand its structure and quality.
2. **Data Ingestion (Bronze Schema):** Ingested raw data into Azure Synapse Analytics.
3. **Data Transformation (Silver Schema):** Cleaned and standardized data using Serverless SQL Pool and Spark Pool.
4. **Data Integration (Gold Schema):** Aggregated and finalized data for analysis.
5. **ETL Pipeline and Scheduling:** Automated data flow using Synapse pipelines.
6. **Data Visualization and Reporting:** Exported data to AWS QuickSight for interactive dashboards.

---



