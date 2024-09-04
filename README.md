# NYC Taxi Azure Data Engineering Project

![New York Taxi Project](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/NYC%20Taxi%20Project.jpg)


### Introduction
In this [Udemy course project](https://www.udemy.com/course/azure-synapse-analytics-for-data-engineers) created by Ramesh Retnasamy, I implemented a data engineering solution using all services available on Azure Synapse Analytics to analyze and report data on Taxi Green trips in New York City from 2020 to 2021.


### What is Azure Synapse Analytics

![Azure Synapse Analytics](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/Azure%20Synapse%20Analytics.jpg)

Azure Synapse Analytics is a powerful cloud-based analytics service provided by Microsoft Azure. It integrates and combines big data and data warehousing capabilities into a unified platform. Synapse Analytics allows you to analyze large volumes of structured and unstructured data from various sources, such as databases, data lakes, and streaming data.

With Synapse Analytics, you can perform data ingestion, data preparation, data warehousing, big data processing, and data exploration all in one place. It provides tools and services for data integration, data wrangling, and data orchestration, making it easier to manage complex data workflows. 

### About the Dataset
![NYC Taxi Green](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/NYC%20Taxi%20Green.jpg)

This dataset contains information about NYC Green trips from 2020 and 2021. - [NYC Green Trip Data](https://1drv.ms/u/s!Aku-bu-I9uuYiFZRUJvhqGJ_Q9sM?e=4Fke0N)

The data was transformed to different file types, and I worked with them in the Synapse Workspace to ingest (Extract), Tranform, and Load data using Serverless Pool:

**Dimension Table**
1. Taxi  Zone 
    - CSV file with header 
    - CSV file without header
2. Calendar
    - CSV file
3. Vendor
    - CSV file
    - CSV file with escaped characters( \ )
    - CSV file with unquote (" ") 
4. Rate Code
    - TSV, Tab-Separated
5. Trip Type
    - JSON file 
6. Payment Type
    - JSON file 
    - JSON file with array 


**Fact Table**

7. Trip Data Green 
    - CSV file with Partitioned by Year and Month 
    - Parquet file with Partitioned by Year and Month 
    - Delta file with Partitioned by Year and Month 

![Data Overview](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/Taxi%20Green%20Trip%20Data%20Overview.png)

### Services Used
1. **Serverless SQL Pool:** The Serverless SQL Pool in Azure Synapse Analytics is a powerful feature that enables you to query large volumes of data without the need for provisioning or managing dedicated resources. It offers an on-demand, serverless approach to executing SQL queries against data stored in various formats within your Azure Data Lake Storage Gen2.
 
#### Solution Architecture 
<img width="1372" alt="image" src="https://github.com/user-attachments/assets/f03937ef-754a-4caf-8b41-fa59f3036c58">


2. **Apache Spark Pool:** The Apache Spark Pool in Azure Synapse Analytics is a powerful computing resource designed for large-scale data processing and analytics. It provides a highly scalable and distributed processing engine, allowing users to execute Spark jobs on massive datasets. The Spark Pool leverages the Apache Spark framework, enabling users to perform complex data transformations, machine learning tasks, and advanced analytics. With seamless integration into Azure Synapse Analytics, it offers a unified environment for data ingestion, exploration, and visualization, empowering organizations to gain valuable insights from their data quickly and efficiently.

#### Solution Architecture
<img width="1350" alt="image" src="https://github.com/user-attachments/assets/2bae5b95-3883-4937-986e-9cae29386720">

3. **Synapse Link:** Synapse Link is a feature within Azure Synapse Analytics that enables seamless and real-time integration between the cloud-based analytical capabilities of Azure Synapse and operational data stored in Azure Cosmos DB. It eliminates the need for data movement or duplication, allowing organizations to directly query and analyze live data from Azure Cosmos DB using familiar SQL-based tools and techniques. Synapse Link provides a unified and efficient way to bridge the gap between analytical and operational systems, enabling organizations to gain valuable insights from their real-time data without compromising on performance or scalability.

#### Solution Architecture
![Synapse Link](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/Synapse%20Link%20Solution%20Arch.png)

4.  **Data Visualization and Analysis:**

This project demonstrates the strategic use of different cloud environments based on specific needs within the data engineering cycle. The final, processed data (gold data) from Synapse was imported into an S3 bucket within the AWS Cloud in a partitioned format for querying and visualization in the final reports.

AWS Glue: Used to crawl the partitioned data and automatically identify the schema, considering the partitioned columns (Month and Year).

AWS Athena: This serverless query engine was utilized to create an external table over the data files, enabling seamless interaction with the data using standard SQL queries.

AWS QuickSight: The final table was ingested into this powerful business intelligence service. By leveraging QuickSight's interactive dashboards and visualizations, key trends and patterns within the NYC Taxi Green trip data were identified, directly supporting the project's campaign requirements.

#### AWS Quicksight Report

<img width="1159" alt="image" src="https://github.com/user-attachments/assets/371deec9-7e29-49f0-9d69-420e351b0a2c">

<img width="1073" alt="image" src="https://github.com/user-attachments/assets/fad9779c-a9ba-4256-912e-84224a770af1">


### Project Execution Flow 
**Main Steps**
1. Discovery (Exploratory Data Analsys with T-SQL) 
2. Create External Table (Bronze Schema) to ingest (Extract) data and create External Table/View. 
3. Create External Table (Silver Schema) to transform data in the appropriate format. 
4. Create External Table (Gold Schema) to join table for use for AWS Quicksight and for keeping the data in a Data Warehouse in Synapse.
5. Create ETL Pipeline and schedule trigger runs (from Bronze to Gold).

**Additional Steps**
1. Transform data with Apache Spark Pool (Notebooks).
2. Query data from Azure Cosmos DB (for real-time data and saving as JSON files) 
3. Provision Dedicated SQL Pool to keep final data (Gold). 
4. Create Power BI report, connecting it to Azure Synapse Analytics, and publish to AWS Quicksight.
