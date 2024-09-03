/*

Business Requirements:

Increase credit card payments: Target customers to encourage credit card usage.
Track payment ratio: Measure the percentage of cash vs. credit card payments.
Identify payment patterns: Analyze payment behavior by day and borough.

Non-Functional Requirements:

Data volume: Handle hundreds of millions of trips per month.
Incremental updates: Process new data monthly.
Performance: Ensure efficient data access and reporting.
Partitioning: Partition data by year and month for efficient querying.

Data Requirements:

Trip data: Trip information, including timestamps, payment types, and boroughs.
Calendar data: Day of the week and weekday/weekend flags.

Gold Layer Table Structure:

Columns: year, month, cash_trip_count, card_trip_count, day_of_week, weekday_weekend_flag, borough.
Partitioning: Partition by year and month.

Data Sources:

Trip data: Trip data file.
Borough information: Taxonomies file.
Calendar data: Derived from trip dates.

Transformation Logic:

Calculate trip counts: Group trip data by payment type and date.
Determine day of week and weekday/weekend flag: Use calendar data to classify trips.
Identify borough: Extract borough information from trip data.

Additional Considerations:

Data Quality: Implement data validation and cleansing.
Performance Optimization: Consider indexing and query optimization techniques.
Scalability: Ensure the data warehouse can handle the expected data volume.
Security: Implement appropriate security measures to protect sensitive data.

*/

USE nyc_taxi_ldw;
GO

-- Lets start with the select statement required to satisfy the campaigns requirement

-- turn the select statement into a stored procedure to call it from this script for incremental new data
-- we have a view with the partitions, use this view so we can prune partitions

-- we want only the date from the lpep_pickup_datetime, to have the trip_date column , not the time details
-- we take the years and months as we want the partitions
-- we join with taxi zone silver table as it has the boroughs
-- we join with calendar to see if the date corresponds to weekend or weekday
-- we use the CASE statement to mark a flag if it is weekend
-- we join with the payment_type table to find the credit card and cash payments
-- we need the number of trips made for each payment method

/*
SELECT 
        td.year,
        td.month,
        tz.borough,
        convert(date, td.lpep_pickup_datetime) as trip_date,
        cal.day_name as trip_day,
        CASE WHEN cal.day_name IN ('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END as trip_day_weekend_ind,
        SUM(CASE WHEN pt.description = 'Credit card' THEN 1 ELSE 0 END) as card_trip_count,
        SUM(CASE WHEN pt.description = 'Cash' THEN 1 ELSE 0 END) as cash_trip_count
FROM silver.vw_trip_data_green td
JOIN silver.taxi_zone tz ON (td.pu_location_id = tz.location_id)
JOIN silver.calendar cal ON (convert(date, td.lpep_pickup_datetime) = cal.date)
JOIN silver.payment_type pt ON (td.payment_type = pt.payment_type)
WHERE td.year = '2020'
    AND td.month = '01' 
GROUP BY td.year,
        td.month,
        tz.borough,
        convert(date, td.lpep_pickup_datetime),
        cal.day_name 


*/

-- this select statement will be used to form the stored procedure in order to be used for every year and month 
-- variables (new partitions, incrementally loaded)


-- execute the stored procedure created in 3_usp_gold_trip_data_green , as per this select statement here


EXEC gold.usp_gold_trip_data_green '2020', '01'
EXEC gold.usp_gold_trip_data_green '2020', '02'
EXEC gold.usp_gold_trip_data_green '2020', '03'
EXEC gold.usp_gold_trip_data_green '2020', '04'
EXEC gold.usp_gold_trip_data_green '2020', '05'
EXEC gold.usp_gold_trip_data_green '2020', '06'
EXEC gold.usp_gold_trip_data_green '2020', '07'
EXEC gold.usp_gold_trip_data_green '2020', '08'
EXEC gold.usp_gold_trip_data_green '2020', '09'
EXEC gold.usp_gold_trip_data_green '2020', '10'
EXEC gold.usp_gold_trip_data_green '2020', '11'
EXEC gold.usp_gold_trip_data_green '2020', '12'

-- For the first six months of 2021
EXEC gold.usp_gold_trip_data_green '2021', '01'
EXEC gold.usp_gold_trip_data_green '2021', '02'
EXEC gold.usp_gold_trip_data_green '2021', '03'
EXEC gold.usp_gold_trip_data_green '2021', '04'
EXEC gold.usp_gold_trip_data_green '2021', '05'
EXEC gold.usp_gold_trip_data_green '2021', '06'

-- we will again create a view for these gold partitions 
-- as the external table in serverless sql does not support partition pruning
