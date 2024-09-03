-- scan specific columns to get advantage of parquet file format
-- even though openrowset finds the correct schema from the file's metadata,
-- its better to define our own data types for cost and performance
-- especially when querying a lot through serverless sql pools


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]


-- find the system default data types after openrowset identifies the schema from the metadata
EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''trip_data_green_parquet/year=2020/month=01/'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''PARQUET''
    ) AS [result]
    '

-- define columns and data types

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
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
        congestion_surcharge FLOAT
    ) AS trip_data


-- use wildcards to select folders/ subfolders

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        -- all the files in folder /*
        -- all parquet files in folder /*.csv
        -- all months of a year month = *
        BULK 'trip_data_green_parquet/year=2020/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) WITH (
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
        congestion_surcharge FLOAT
    ) AS trip_data



-- use filepath function to select only from certain partitions

-- year is the first subfolder of the file's path
-- month is the second subfolder
-- where year is 2020 and months are 2,4,6
-- we make FULLY USE of the partitions, the specific columns of parquet utilization and metadata functions!

-- we can use the filepath() like that ONLY when we use wildcards in our partitions
-- for example trip_data_green_parquet/year=*/month=*/*.parquet
-- we choose all years all months all parquet files

SELECT
    TOP 100 
    trip_data.filepath(1) as year,
    trip_data.filepath(2) as month,
    count(1) as record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) WITH (
      VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT
    ) AS trip_data
WHERE trip_data.filepath(1) = 2020
    AND trip_data.filepath(2) in ('02', '04', '06')
GROUP BY trip_data.filepath(1), trip_data.filepath(2)
ORDER BY trip_data.filepath(1), trip_data.filepath(2)



