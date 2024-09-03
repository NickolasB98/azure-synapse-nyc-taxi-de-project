-- whats new here?!
-- the 2 new columns year, month
-- automatically created from the delta's metadata
-- make the partitions
-- limit data with WITH clause based on these columns
-- metadata filepath() function no longer needed for that

-- ALWAYS MAKE USE OF PARTITIONS TO FILTER DATA

-- IMPORTANT we can no longer put a subfolder in BULK , only the delta folder 
-- that contains the delta logs

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_delta/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) AS trip_data

-- lets check the data types
-- new columns month year are varchar(8000)

EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''trip_data_green_delta/'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''DELTA''
    ) AS trip_data
    '
-- again best practice to manually put the data types of columns

SELECT 
    TOP 100 *
FROM 
    OPENROWSET(
        BULK 'trip_data_green_delta/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) 
WITH (
    VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT,
        year VARCHAR(4),
        month VARCHAR(2)
    )
    AS trip_data


-- when choosing specific columns in the query
-- DO NOT get rid of the columns made by the partitions
-- year and month here
-- delta can only work successfully while including these partitioning columns


SELECT 
    TOP 100 *
FROM 
    OPENROWSET(
        BULK 'trip_data_green_delta/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) 
WITH (
    VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        year VARCHAR(4),
        month VARCHAR(2)
    )
    AS trip_data;

-- take advantage in cost and performance
-- using the partitions as filters in where clause

SELECT 
    count(distinct payment_type) 
FROM 
    OPENROWSET(
        BULK 'trip_data_green_delta/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    ) as trip_data
WHERE year = '2020' AND month = '03';