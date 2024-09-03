USE nyc_taxi_ldw
GO

-- we do not include all the columns 
-- as we already had month and year in the bronze view
-- we skip them because we create them again here with the filepath function


DROP VIEW IF EXISTS silver.vw_trip_data_green
GO

CREATE VIEW silver.vw_trip_data_green
AS
SELECT
    result.filepath(1) as year,
    result.filepath(2) as month,
    result.*
FROM
    OPENROWSET(
        BULK 'silver/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'PARQUET'
    ) 
    WITH (
      vendor_id INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        rate_code_id INT,
        pu_location_id INT,
        do_location_id INT,
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

select top 100 * from silver.vw_trip_data_green
where year = '2020' and month = '05';