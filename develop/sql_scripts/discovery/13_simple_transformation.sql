-- eg format a date field
-- format a string
-- combibe different string fields eg full name
-- derive something from 2 different columns etc


-- eg we want Number of Trips Made by duration in HOURS
-- between 0-1 hours , how many trips
-- between 1h - 2h how many trips etc
-- a way of clustering


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data


-- we got pickup time and dropoff time
-- datediff (dropoff - pickup) = duration

-- lets find the trips that happened for 1 hour = from_hour 0 to_hour 1
-- the trips that have duration 1-2 hours are from_hour 1 to_hour 2
-- and so on

-- we choose the datediff(minute, starting hour, finish hour) 
-- so we divide by 60 min to find the hours
-- add +1 to reach the next hour

SELECT
    DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60 as from_hour,
    (DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1 as to_hour,
    count(1) as number_of_trips

FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
WHERE lpep_pickup_datetime < lpep_dropoff_datetime
GROUP BY DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60,
        (DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1
ORDER BY from_hour, to_hour

-- if we want to check the quality issues here and not end up with negative differences
-- we have to check always the lpep_pickup_datetime < lpep_dropoff_datetime


