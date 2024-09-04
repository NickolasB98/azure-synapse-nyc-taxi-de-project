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

##### Pipelines created in Synapse:

**pl_create_silver_tables:**

Creates the silver tables for all the non-partitioned data. Uses the For Each function to capture the metadata of the files and stored procedures, to delete the previous tables and recreate them according to the existing / updated metadata filenames and stored procedures. This way, the pipelines are **Idempotent**: Idempotency in data pipelines refers to the ability to execute the same operation multiple times without changing the result beyond the initial application.

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/8b3a0ca7-dcfc-40da-a26f-cc1971792aec">


**pl_create_silver_trip_data_green:**

Creates the silver tables for the partitioned data. Uses the Script that gets the partitions from the bronze view created for the trip data file, to use inside the ForEach function. The function follows the same logic as before, deleting the previous partitions and recreating them according to the existing / updated partitions as received from the bronze. It finally creates the silver view, which allows for partition pruning.

<img width="1437" alt="image" src="https://github.com/user-attachments/assets/c9972f74-8511-43f3-89c3-ac7107c0b3de">


**pl_create_gold_trip_data_green:**

Creates the final gold view. Uses that Script that gets the partitions from the silver view created by the previous pipeline, to use inside the ForEach function. The functions acts accordingly, deleting the previous partitions and recreating them based on the updated / existing partitions as taken from the updated silver view. It finally created the golden view, having joined the dimension tables to the fact table and the data is aggregated to represent important business insights. The gold view supports partition pruning, to benefit from querying and filtering, utilizing the partitions of Year and Month.

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/e04589f6-4557-43a5-b307-06a29bf65bd2">


#### The Master Pipeline

Used to connect all of the previous pipelines, form dependencies between them, and utilize a Schedule type Trigger to get executed automatically.

<img width="1438" alt="image" src="https://github.com/user-attachments/assets/dfc2821d-2a36-4651-bde9-5d79416edd25">

<img width="631" alt="image" src="https://github.com/user-attachments/assets/ffc27d0a-aa82-4cf7-af54-7a7ee9310089">

#### Important Limitation of Synapse Serverless SQL:

The use of Serverless SQL for the Transformation phase does not allow the maintenance of partitions, as initially imported from the Bronze layer. To prevent this issue, I used a dynamic dataset and Stored Procedures to recreate the partition parquet files. This means I used one stored procedure for every partition existing in every pipeline execution. This problem can not scale very efficiently if the data grows bigger each time. The solution is an Architecture using Spark Pool for the transformations.

**The Solution**

**2. Apache Spark Pool:** The Apache Spark Pool in Azure Synapse Analytics is a powerful computing resource designed for large-scale data processing and analytics. It provides a highly scalable and distributed processing engine, allowing users to execute Spark jobs on massive datasets. The Spark Pool leverages the Apache Spark framework, enabling users to perform complex data transformations, machine learning tasks, and advanced analytics. With seamless integration into Azure Synapse Analytics, it offers a unified environment for data ingestion, exploration, and visualization, empowering organizations to gain valuable insights from their data quickly and efficiently.

#### Solution Architecture
<img width="1350" alt="image" src="https://github.com/user-attachments/assets/2bae5b95-3883-4937-986e-9cae29386720">

**Spark Pool Configuration**

To initialize a Spark Pool, specific configurations are required. As a minimum, it's recommended to deploy one master node and at least two worker nodes.

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/e09613e7-3ef4-4a9d-bd24-cfd4b77d0b70">

**Spark Notebook Integration**

Once configured, the Spark Pool is readily accessible by notebooks, allowing them to leverage its processing power. In this project, a Spark notebook transforms silver data into the final, gold-tier aggregated format. This approach efficiently utilizes existing partitions within the silver trip_data_green parquet files. This simple yet effective solution eliminates the need for intricate dynamic datasets in Synapse Data Factory or dynamic stored procedures that rely on variable partition names.

By incorporating the Spark notebook execution pipeline within the master pipeline, this project achieves significantly faster, more scalable, and easier-to-maintain data transformations. This demonstrates the optimal utilization of Serverless SQL Pool and Spark Pool, where they can seamlessly collaborate within the Azure Synapse Analytics environment.

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/5ff7dc2c-5292-4fd4-9e5a-d0bdd9fd2a63">

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/f5b85bf2-12a3-4bff-bd7b-4bb6a0060e56">

### 3. Data Visualization and Analysis:

This project demonstrates the strategic use of different cloud environments based on specific needs within the data engineering cycle. The final, processed data (gold data) from Synapse was imported into an S3 bucket within the AWS Cloud in a partitioned parquet format for querying and visualization in the final reports.

**S3 Bucket containing the Gold Partinioned Data**:

<img width="1296" alt="s3Bucket" src="https://github.com/user-attachments/assets/75f0c5ec-cc4d-4854-bd4e-898615487c51">
<img width="1276" alt="s3Partitions" src="https://github.com/user-attachments/assets/86cd80c4-df5c-4ec7-b614-ddcb69694a24">


**AWS Glue:** Used to crawl the partitioned data and automatically identify the schema, considering the partitioned columns (Month and Year).

<img width="1094" alt="Crawler" src="https://github.com/user-attachments/assets/408fc306-e3c4-4889-8835-1f32c364ea52">

<img width="1059" alt="tableAfterCrawler" src="https://github.com/user-attachments/assets/5c556e64-74a0-4a34-954a-2b516a7d6a5c">



**AWS Athena:** This serverless query engine was utilized to create an external table over the data files, enabling seamless interaction with the data using standard SQL queries.

<img width="1264" alt="tableInAthena" src="https://github.com/user-attachments/assets/4a74e889-c6b9-479a-bc55-98e5bec2c362">


**AWS QuickSight:** The final table was ingested into this powerful business intelligence service. By leveraging QuickSight's interactive dashboards and visualizations, key trends and patterns within the NYC Taxi Green trip data were identified, directly supporting the project's campaign requirements.

<img width="709" alt="QuicksightDashboard" src="https://github.com/user-attachments/assets/7f51712a-ed88-4c69-bca4-5fa090ee235f">

### Business Requirements

<img width="850" alt="image" src="https://github.com/user-attachments/assets/a3d13e47-8250-4bff-a81d-1d6f8d8faeb0">

<img width="853" alt="image" src="https://github.com/user-attachments/assets/93462716-2a0a-4365-a160-0da6bd92d2a9">


### AWS Quicksight Report Based on Requirements

<img width="1159" alt="image" src="https://github.com/user-attachments/assets/371deec9-7e29-49f0-9d69-420e351b0a2c">

<img width="1073" alt="image" src="https://github.com/user-attachments/assets/fad9779c-a9ba-4256-912e-84224a770af1">


### Project Execution Flow 

#### Core Stages:

**Discovery and Exploration:** A thorough analysis of the dataset was conducted using T-SQL to gain a comprehensive understanding of the data's structure, quality, and potential insights.

**Data Ingestion (Bronze Schema):** The raw data was extracted from its source and ingested into a Bronze Schema external table within Azure Synapse Analytics. This stage serves as the initial landing ground for the data.

**Data Transformation (Silver Schema):** The data in the Bronze Schema was transformed to a more refined and usable format in the Silver Schema. This step involved cleaning, standardizing, and enriching the data to ensure its quality and consistency.

**Data Integration and Finalization (Gold Schema):** The transformed data from the Silver Schema was integrated into the Gold Schema, where the final, refined dataset was created. This step involved joining tables, aggregating data, and applying any necessary calculations or transformations to prepare the data for analysis and reporting.

**ETL Pipeline and Scheduling:** An ETL (Extract, Transform, Load) pipeline was established to automate the data flow from Bronze to Gold Schemas. This pipeline was scheduled to run regularly, ensuring the data was continuously updated and refreshed.

### Additional Steps:

Data Transformation with Apache Spark: The data was processed using Apache Spark Notebooks to perform complex transformations or analyses that were not feasible or efficient using T-SQL.

Data Visualization and Reporting (QuickSight): The final dataset was ingested into an AWS S3 Bucket, queried into AWS Athena, and finally ingested into AWS QuickSight, a powerful business intelligence service. QuickSight's interactive dashboards and visualizations were used to explore the data, identify key trends and patterns, and generate insightful reports to support the project's objectives.


<img width="853" alt="image" src="https://github.com/user-attachments/assets/3dd4d023-2932-44ff-a3dc-8915f14618b4">

**Synapse Link:** Synapse Link is a feature within Azure Synapse Analytics that enables seamless and real-time integration between the cloud-based analytical capabilities of Azure Synapse and operational data stored in Azure Cosmos DB. It eliminates the need for data movement or duplication, allowing organizations to directly query and analyze live data from Azure Cosmos DB using familiar SQL-based tools and techniques. Synapse Link provides a unified and efficient way to bridge the gap between analytical and operational systems, enabling organizations to gain valuable insights from their real-time data without compromising on performance or scalability.

#### Solution Architecture
![Synapse Link](https://github.com/okaforoa/nyc-taxi-azure-data-engineering-project/blob/main/images/Synapse%20Link%20Solution%20Arch.png)

Real-Time Data Integration (Azure Cosmos DB): For real-time data requirements, data was queried from Azure Cosmos DB and saved as JSON files. The files were automatically converted into columnar storage format and stored in an OLAP Warehouse automatically created by Synapse Link. This enabled integration with real-time data sources and provided a more up-to-date view of the data without the need of ETL. This chapter is not included in the main project but is an additional step to demonstrate how OLTP data automatically inserted from taxi IOT machines can become almost instantly available for historical big data analysis in the OLAP warehouse.

###### Let's show the steps for this seamless integration of CosmosDB, Serverless SQL Pool, and Spark Pool in Azure Synapse, with the use of Synapse Link.

Data from IOT Taxi Devices are imported into the Heartbeat container in Cosmos DB.
Synapse Link is enabled in CosmosDB configuration, which will allow the transformation of OLTP row storage data to OLAP columnar without the need of ETL.
<img width="1433" alt="insertItemsMongoDB" src="https://github.com/user-attachments/assets/2c6565fa-831d-4607-b313-255f6d6e0077">

The data is now Linked inside Synapse, ready to be queried from our Serverless SQL Pool or Spark Pool.
<img width="297" alt="LinkedDataSynapse" src="https://github.com/user-attachments/assets/4313662b-b3df-4697-88fd-d3c2759223cf">

The data are seamlessly queried directly from CosmosDB, efficiently getting advantage of their columnar parquet format for faster queries and no need to further ETL steps.

<img width="1438" alt="queryHeartbeatFromServerlessPool" src="https://github.com/user-attachments/assets/d8bba9f9-d64d-4cbc-912a-ce6a9765f0c1">

In this Spark notebook, the data is read from the OLAP warehouse, and getting transformed into a PySpark Dataframe

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/6b31076b-be8d-4fb0-a7e8-c11d4c794b63">

This usecase shows how the data imported from IOT devices in Cosmos DB can be instantly transformed in a spark Dataframe and provide direct insights to the end-analysts.

<img width="1439" alt="image" src="https://github.com/user-attachments/assets/3ca024c2-032b-4cfe-8dbd-77e4f3191b24">







