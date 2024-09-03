-- join datasets to get our desired output

-- num of trips made for each borough

-- we have location id but not borough name in trip data
-- we have borough name in the taxi zone file, which also has the location id 


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS [result]
WHERE PULocationID is null
-- we have not nulls pick up location ids
-- use the PICK UP location ID

-- we want number of trips made FROM each borough so pick up location

SELECT
    taxi_zone.borough,
    count(1) as trips_per_borough
FROM
    OPENROWSET(
                BULK 'trip_data_green_parquet/year=2020/month=01/',
                DATA_SOURCE = 'nyc_taxi_data_raw',
                FORMAT = 'PARQUET'
            ) as trip_data
    JOIN OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2
        ) 
        WITH (
            location_id SMALLINT 1,
            borough VARCHAR(15) 2,
            zone VARCHAR(50) 3,
            service_zone VARCHAR(15) 4
        ) as taxi_zone
    ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY taxi_zone.borough
ORDER BY count(1) DESC
