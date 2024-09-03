USE nyc_taxi_ldw
GO

-- Create calendar silver table
IF OBJECT_ID('silver.calendar') IS NOT NULL
    DROP EXTERNAL TABLE silver.calendar
GO

CREATE EXTERNAL TABLE silver.calendar 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/calendar',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.calendar

GO 


select top 100 * from silver.calendar



-- Create vendor silver table
IF OBJECT_ID('silver.vendor') IS NOT NULL
    DROP EXTERNAL TABLE silver.vendor
GO

CREATE EXTERNAL TABLE silver.vendor 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/vendor',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.vendor

GO 

select top 100 * from silver.vendor



-- Create trip type silver table
IF OBJECT_ID('silver.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.trip_type
GO

CREATE EXTERNAL TABLE silver.trip_type 
    WITH (
            DATA_SOURCE = nyc_taxi_src,              
            LOCATION = 'silver/trip_type',  
            FILE_FORMAT = parquet_file_format
    )
AS SELECT * 
    FROM bronze.trip_type

GO 

select top 100 * from silver.trip_type

