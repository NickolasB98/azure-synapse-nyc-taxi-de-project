-- to keep our ability to prune partitions
-- we cannot just reuse the select statement we built the golden table
-- where we just keep the columns of year and month
-- we do not chose them in the columns statement, we recreate them
-- using again the result.filepath function

USE nyc_taxi_ldw
GO

DROP VIEW IF EXISTS gold.vw_trip_data_green
GO

CREATE VIEW gold.vw_trip_data_green
AS
SELECT
    result.filepath(1) as year,
    result.filepath(2) as month,
    result.*
FROM
    OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        borough                 VARCHAR(15),
        trip_date               DATE,
        trip_day                VARCHAR(10),
        trip_day_weekend_ind    CHAR(1),
        card_trip_count         INT,
        cash_trip_count         INT,
        street_hail_trip_count  INT,
        dispatch_trip_count     INT,
        trip_distance           FLOAT,
        trip_duration           INT,
        fare_amount             FLOAT
        ) AS [result]

GO

select * from gold.vw_trip_data_green
where year = '2020' and month = '06'
-- INITIAL DATA COUNT 2,6 MILL
-- AGGREGATED GOLD DATA = 3316 RECORDS

-- now we can see year, month which are the partitions to query the data 
-- we can see borough, trip date, trip day, if it is a weekend or not, payments by card this day and payments by cash 



