-- identify any quality issues in trip total amount
-- lets check for january 2020

SELECT
    MIN(total_amount) as min_total_amount,
    MAX(total_amount) as max_total_amount,
    AVG(total_amount) as avg_total_amount,
    COUNT(1) as total_number_of_records,
    COUNT(total_amount) as non_null_total_number_of_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]

-- lets see if minimum total amount -210.3 is valid and not a data quality issue

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE total_amount < 0 


-- we find that payment_type is always 3 or 4 when the total amount is below 0


-- payment type 3 = No Charge 
-- payment type 4 = Dispute (maybe refunds)



SELECT
    payment_type,
    count(1) as number_of_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE total_amount < 0 
GROUP BY payment_type

-- we find payment_type with NULL value making up of a lot of negative trip amounts
-- most negative trip amounts are indeed payment type 3 and 4, but we have a quality issue 


-- an idea is to map ALL THE NULL PAYMENT TYPES with the payment type 5 = Uknown
-- to be correct in our downstream analysis


-- another idea is to drop all negative values but it depends on the needs of analysis or ML predictions


SELECT
    payment_type,
    count(1) as number_of_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
GROUP BY payment_type

-- there are 116051 null payment type values in just the month of January



