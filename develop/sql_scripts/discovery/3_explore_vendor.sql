SELECT *
FROM
    OPENROWSET(
        BULK 'vendor_unquoted.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS vendor;

-- LLC is not shown in the vendor_name value of vendor_id = 1

-- 1st way to solve = use \ before the delimiter (the ,)

SELECT *
FROM
    OPENROWSET(
        BULK 'vendor_escaped.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        -- use double \\ as it needs to be escaped again 
        ESCAPECHAR = '\\'
    ) AS vendor;

-- 2nd way = use double quotes for the whole value

SELECT *
FROM
    OPENROWSET(
        BULK 'vendor.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        -- if we do not specify the fieldquote its
        -- still working as the default value is " 
        FIELDQUOTE = '"'
    ) AS vendor;

-- if the fieldquote is ' instead of " then we need to actually specify it in the
-- FIELDQUOTE = '''


SELECT *
FROM
    OPENROWSET(
        BULK 'vendor.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE 
    ) AS vendor;
