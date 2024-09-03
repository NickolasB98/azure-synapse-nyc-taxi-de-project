USE nyc_taxi_ldw;
GO

CREATE OR ALTER PROCEDURE silver.usp_silver_taxi_zone
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
        DROP EXTERNAL TABLE silver.taxi_zone;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.taxi_zone 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/taxi_zone',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.taxi_zone;
END;
