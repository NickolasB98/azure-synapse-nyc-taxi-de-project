USE nyc_taxi_ldw
GO

DROP VIEW IF EXISTS bronze.vw_rate_code
GO


CREATE VIEW bronze.vw_rate_code
AS 
SELECT
    rate_code_id,
    rate_code
FROM
    OPENROWSET(
        BULK 'raw/rate_code.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        -- tab raw terminator
        ROWTERMINATOR = '0X0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20) 
    );

GO


select * from bronze.vw_rate_code
GO



DROP VIEW IF EXISTS bronze.vw_payment_type
GO

CREATE VIEW bronze.vw_payment_type
AS 
SELECT 
    payment_type,
    description
FROM
    OPENROWSET(
        BULK 'raw/payment_type.json',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc'
    );
GO

select * from bronze.vw_payment_type
GO

-- WE CANNOT DO PARTINION PRUNING IN EXTERNAL TABLES MADE FOR SERVERLESS SQL
-- this is why we can create a view for our big files which supports partition pruning

DROP VIEW IF EXISTS bronze.vw_trip_data_green_csv
GO

CREATE VIEW bronze.vw_trip_data_green_csv
AS
SELECT
    result.filepath(1) as year,
    result.filepath(2) as month,
    result.*
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'raw/trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
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
        ) AS [result]

GO

select * from bronze.vw_trip_data_green_csv
where year = '2020' and month = '05';
-- 2.3 million records in total
-- this is how we use partition pruning, create manually the year and month
-- from the filepath function as columns, and filter with WHERE clause from view



