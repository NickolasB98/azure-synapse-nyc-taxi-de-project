USE nyc_taxi_ldw;
GO

CREATE PROCEDURE silver.usp_silver_calendar
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.calendar') IS NOT NULL
        DROP EXTERNAL TABLE silver.calendar;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.calendar 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/calendar',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.calendar;
END;

GO

CREATE PROCEDURE silver.usp_silver_vendor
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.vendor') IS NOT NULL
        DROP EXTERNAL TABLE silver.vendor;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.vendor 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/vendor',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.vendor;
END;

GO

CREATE PROCEDURE silver.usp_silver_trip_type
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.trip_type') IS NOT NULL
        DROP EXTERNAL TABLE silver.trip_type;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.trip_type 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/trip_type',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.trip_type;
END;

GO

CREATE PROCEDURE silver.usp_silver_rate_code
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.rate_code') IS NOT NULL
        DROP EXTERNAL TABLE silver.rate_code;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.rate_code 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/rate_code',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.vw_rate_code;
END;

GO

CREATE PROCEDURE silver.usp_silver_payment_type
AS
BEGIN
    -- Check if the external table exists and drop it if it does
    IF OBJECT_ID('silver.payment_type') IS NOT NULL
        DROP EXTERNAL TABLE silver.payment_type;
    

    -- Create the external table
    CREATE EXTERNAL TABLE silver.payment_type 
    WITH (
        DATA_SOURCE = nyc_taxi_src,              
        LOCATION = 'silver/payment_type',  
        FILE_FORMAT = parquet_file_format
    )
    AS 
    SELECT * 
    FROM bronze.vw_payment_type;
END;