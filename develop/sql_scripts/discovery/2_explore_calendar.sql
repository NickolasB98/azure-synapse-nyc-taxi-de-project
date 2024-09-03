-- find the len of columns 

SELECT
    MAX(len(date_key)) AS len_date_key,
    MAX(len(date)) AS len_date,
    MAX(len(year)) AS len_year,
    MAX(len(month)) AS len_month,
    MAX(len(day)) AS len_day,
    MAX(len(day_name)) AS len_day_name,
    MAX(len(day_of_year)) AS len_day_of_year,
    MAX(len(week_of_month)) AS len_week_of_month,
    MAX(len(week_of_year)) AS len_week_of_year,
    MAX(len(month_name)) AS len_month_name,
    MAX(len(year_month)) AS len_year_month,
    MAX(len(year_week)) AS len_year_week
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
    AS [result]



-- final sql query with correct data types


SELECT
    TOP 100 *
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

