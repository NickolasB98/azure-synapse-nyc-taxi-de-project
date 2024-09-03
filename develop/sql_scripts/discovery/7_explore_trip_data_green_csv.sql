-- WHAT WE LEARN
-- PARTITIONS HELP IN PERFORMANCE AND COST
-- SPECIFY FILE NAMES IN THE BULK OPERATOR
-- AND USE WHERE CLOSE WITH THE METADATA FUNCTIONS TO CHOOSE SPECIFIC PARTITIONS
-- PERFORMANCE : GET TO THE DATA QUICKLY
-- COST : READ SPECIFIC PART OF DATA YOU NEED NOT WHOLE FOLDER

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/green_tripdata_2020-01.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]

-- select data from a whole folder instead 

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        -- all the files in folder /*
        -- all csv files in folder /*.csv
        BULK 'trip_data_green_csv/year=2020/month=01/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]


-- select data from all subfolders of a folder (whole year has 12 subfolders for months)

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        -- all the subfolders in folder /**
        BULK 'trip_data_green_csv/year=2020/**',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]


-- select data from specific and not ALL 
-- subfolders of a folder (pick some months from a year eg Jan to Mar)

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        -- all the subfolders in folder /**
        BULK ('trip_data_green_csv/year=2020/month=01/*.csv',
            'trip_data_green_csv/year=2020/month=02/*.csv',
            'trip_data_green_csv/year=2020/month=03/*.csv'),
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]

-- use more than one wildcard to choose all years all months only csv files 

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]


-- what if we want to find the folder or file that stores the data from the query result
-- file metadata function filename()

SELECT
    TOP 100 
    result.filename() as file_name,
    result.*
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]

-- how many records are in each of the files?

SELECT
    result.filename() as file_name,
    count(1) as record_count
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]
GROUP BY result.filename()
ORDER BY result.filename();


-- limit data using filename() metadata function 
-- where result.filename() in ('onefile', 'secondfile')
-- here we want the results from jan 2020 and jan 2021
SELECT
    result.filename() as file_name,
    count(1) as record_count
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]
WHERE result.filename() IN ('green_tripdata_2020-01.csv', 'green_tripdata_2021-01.csv')
GROUP BY result.filename()
ORDER BY result.filename();


-- filepath() function contains the full path of the file, not only the filename
-- parameter 0,1,2,3 takes the position of a value inside the path
-- filepath(1) is year 
-- filepath(2) is month
-- filepath(3) is the specific file


--lets choose data from 2020 june july august!

SELECT
    result.filepath(1) as year,
    result.filepath(2) as month,
    count(1) as record_count
FROM
    OPENROWSET(
        -- all years: year=*
        -- all months: month=*
        -- only csv: *.csv
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]
WHERE result.filepath(1) = '2020'
    AND result.filepath(2) IN ('06', '07', '08')
GROUP BY result.filename(), result.filepath(1), result.filepath(2)
ORDER BY result.filename(), result.filepath(1), result.filepath(2);