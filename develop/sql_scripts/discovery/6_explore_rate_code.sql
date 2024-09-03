--explore a standard json file
-- openjson again turns the json into raws and columns!
-- use openjson instead of json_value
SELECT
    rate_code_id,
    rate_code
    
FROM
    OPENROWSET(
        BULK 'rate_code.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        -- tab raw terminator
        ROWTERMINATOR = '0X0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20) 
    );




-- multi-line json is the same as the standard json file
-- the row terminator needs to be overriden as 0x0b 
-- to be a tab delimiter instead of comma


SELECT
    rate_code_id,
    rate_code
    
FROM
    OPENROWSET(
        BULK 'rate_code_multi_line.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        -- tab raw terminator
        ROWTERMINATOR = '0X0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20) 
    );