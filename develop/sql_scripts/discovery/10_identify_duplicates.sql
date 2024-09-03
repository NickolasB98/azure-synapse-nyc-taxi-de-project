-- check if primary key or primary combination keys have duplicate values 

SELECT
    location_id,
    count(1) as number_of_records
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        -- ignore the first column , we dont want location to be a smallint , only its values!
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
    AS taxi_zone
GROUP BY location_id
HAVING count(1) > 1


SELECT
    date_key,
    count(1) as number_of_records
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
-- tinyint = 0-255 smallint = -32768 - 32767 int = -2mill - 2mill
WITH (
        date_key INT,
        date DATE,
        year SMALLINT,
        month TINYINT,
        day TINYINT,
        day_name VARCHAR(10),
        day_of_year SMALLINT,
        week_of_month TINYINT,
        week_of_year TINYINT,
        month_name VARCHAR(10),
        year_month INT,
        year_week INT
    )
    AS calendar
GROUP BY date_key
HAVING count(1) > 1


