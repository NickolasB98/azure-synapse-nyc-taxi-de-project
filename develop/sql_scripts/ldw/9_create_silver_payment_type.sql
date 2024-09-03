USE nyc_taxi_ldw;
GO 

-- Create payment type silver table
IF OBJECT_ID('silver.payment_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.payment_type
GO

CREATE EXTERNAL TABLE silver.payment_type 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/payment_type',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.vw_payment_type

GO 

select top 100 * from silver.payment_type

