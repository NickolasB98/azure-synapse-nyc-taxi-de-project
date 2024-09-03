-- Firstly, we need to create the stored procedure to accept the partition column as inputs.
-- The stored procedure uses the CETAS statement to transform the data for the specific partition from
-- CSV to PARQUET and create the external table.
-- We then have to drop the external table for the specific partition as it's not required.
-- Once this procedure is created, we need to execute this load procedure for each of the partitions while
-- hardcore the partition values in this section of the course.
-- Last, we need to create a view for the entire data and also include the partition columns so
-- that we can prune the partitions.

USE nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE silver.usp_silver_trip_data_green
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN

    DECLARE @create_sql_stmt NVARCHAR(MAX),
            @drop_sql_stmt   NVARCHAR(MAX);

    SET @create_sql_stmt = 
        'CREATE EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month + 
        ' WITH
            (
                DATA_SOURCE = nyc_taxi_src,
                LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month + ''',
                FILE_FORMAT = parquet_file_format
            )
        AS
        SELECT [VendorID] AS vendor_id
                ,[lpep_pickup_datetime]
                ,[lpep_dropoff_datetime]
                ,[store_and_fwd_flag]
                ,[total_amount]
                ,[payment_type]
                ,[trip_type]
                ,[congestion_surcharge]
                ,[extra]
                ,[mta_tax]
                ,[tip_amount]
                ,[tolls_amount]
                ,[ehail_fee]
                ,[improvement_surcharge]
                ,[RatecodeID] AS rate_code_id
                ,[PULocationID] AS pu_location_id
                ,[DOLocationID] AS do_location_id
                ,[passenger_count]
                ,[trip_distance]
                ,[fare_amount]
        FROM bronze.vw_trip_data_green_csv
        WHERE year = ''' + @year + '''
          AND month = ''' + @month + '''';    

    print(@create_sql_stmt)

    EXEC sp_executesql @create_sql_stmt;

    SET @drop_sql_stmt = 
        'DROP EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month;

    print(@drop_sql_stmt)
    EXEC sp_executesql @drop_sql_stmt;

END;



