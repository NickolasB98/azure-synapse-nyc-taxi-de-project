-- even though this csv has partitions for years and months
-- in serverless sql CETAS can turn it into one partition table only
-- this slows down the queries if we do big data
-- this can be solved with stored procedures by creating tables for EVERY partition
-- but it is a bad work-around, the real solution = spark pool SQL to create partitions 


/*

USE nyc_taxi_ldw;
GO 

-- Create trip data green silver table
IF OBJECT_ID('silver.trip_data_green') IS NOT NULL
    DROP EXTERNAL TABLE silver.trip_data_green
GO

CREATE EXTERNAL TABLE silver.trip_data_green 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/trip_data_green',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.trip_data_green_csv

GO 

select top 100 * from silver.trip_data_green

*/

-- lets do the bad solution that creates the partitions through stored procedures
-- good solution is using spark pool later on


-- For all months of 2020
EXEC silver.usp_silver_trip_data_green '2020', '01'
EXEC silver.usp_silver_trip_data_green '2020', '02'
EXEC silver.usp_silver_trip_data_green '2020', '03'
EXEC silver.usp_silver_trip_data_green '2020', '04'
EXEC silver.usp_silver_trip_data_green '2020', '05'
EXEC silver.usp_silver_trip_data_green '2020', '06'
EXEC silver.usp_silver_trip_data_green '2020', '07'
EXEC silver.usp_silver_trip_data_green '2020', '08'
EXEC silver.usp_silver_trip_data_green '2020', '09'
EXEC silver.usp_silver_trip_data_green '2020', '10'
EXEC silver.usp_silver_trip_data_green '2020', '11'
EXEC silver.usp_silver_trip_data_green '2020', '12'

-- For the first six months of 2021
EXEC silver.usp_silver_trip_data_green '2021', '01'
EXEC silver.usp_silver_trip_data_green '2021', '02'
EXEC silver.usp_silver_trip_data_green '2021', '03'
EXEC silver.usp_silver_trip_data_green '2021', '04'
EXEC silver.usp_silver_trip_data_green '2021', '05'
EXEC silver.usp_silver_trip_data_green '2021', '06'

-- we now created parquet files and dropped the individual external tables for every month and year

-- it was hardcoded but if we create a pipeline with these variables it is easy to become automated




