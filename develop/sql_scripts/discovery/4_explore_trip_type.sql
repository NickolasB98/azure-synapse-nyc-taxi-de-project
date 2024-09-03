-- it is a tsv = tab seperated 

SELECT *
FROM
    OPENROWSET(
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '\t' --tab 
    ) AS trip_type;
    