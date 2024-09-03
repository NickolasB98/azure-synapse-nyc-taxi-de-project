USE nyc_taxi_ldw;
GO 

-- Create rate code silver table
IF OBJECT_ID('silver.rate_code') IS NOT NULL
    DROP EXTERNAL TABLE silver.rate_code
GO

CREATE EXTERNAL TABLE silver.rate_code 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/rate_code',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.vw_rate_code

GO 

select top 100 * from silver.rate_code



