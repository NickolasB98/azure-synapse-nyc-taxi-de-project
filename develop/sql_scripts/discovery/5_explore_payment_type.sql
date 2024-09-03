-- use openjson to bring the json into a columns-rows format
-- cross apply is an inner join , that is why
-- we select only the columns created after the cross apply join
-- rename the column names by column_name '$.new_column_name'

SELECT 
    payment_type,
    description
FROM
    OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc'
    );

-- use openjson to explode an array inside a json value 
-- we tread the whole array as a json object so we 
-- use openjson again inside the array and explode the values 

SELECT 
    payment_type,
    payment_type_desc_value
FROM
    OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) 
    WITH
    (
        jsonDoc NVARCHAR(MAX)
    ) AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        payment_type_desc NVARCHAR(MAX) AS JSON 
    )
    CROSS APPLY OPENJSON(payment_type_desc)
    WITH(
        sub_type SMALLINT,
        payment_type_desc_value VARCHAR(20) '$.value'
    );