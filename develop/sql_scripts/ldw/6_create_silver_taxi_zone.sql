-- we use a CETAS to create an EXTERNAL table 
-- we need a data source, the same as when we created the bronze tables
-- we need a new location we created silver
-- we used the same parquet file format we had created for the previous parquet file
-- after the configs, we write AS SELECT and based on this query the silver table is created 


USE nyc_taxi_ldw
GO

-- Create taxi_zone silver table
IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE silver.taxi_zone
GO

CREATE EXTERNAL TABLE silver.taxi_zone 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/taxi_zone',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.taxi_zone

GO

-- this script is not rerunnable without a pipeline, as we need to manually delete the data 
-- of the silver/taxi_zone location before recreating the same table
-- we will use an adf pipeline to delete this data automatically

select * from silver.taxi_zone