-- create external file format

USE nyc_taxi_ldw;
GO

-- Create an external file format
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format')
    CREATE EXTERNAL FILE FORMAT csv_file_format
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,  -- Specify the format type (e.g., DELIMITEDTEXT, PARQUET, etc.)
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',       -- Specify the field terminator for delimited text
            STRING_DELIMITER = '"',       -- Specify the string delimiter for text
            FIRST_ROW = 2  ,              -- Specify the first row to read (if headers are present)
            USE_TYPE_DEFAULT = FALSE,
            ENCODING = 'UTF8',
            PARSER_VERSION = '2.0'         
        )
    );


-- Create an external file format with parser version 1.0 to handle rejections
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format_pv1')
    CREATE EXTERNAL FILE FORMAT csv_file_format_pv1
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,  -- Specify the format type (e.g., DELIMITEDTEXT, PARQUET, etc.)
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',       -- Specify the field terminator for delimited text
            STRING_DELIMITER = '"',       -- Specify the string delimiter for text
            FIRST_ROW = 2  ,              -- Specify the first row to read (if headers are present)
            USE_TYPE_DEFAULT = FALSE,
            ENCODING = 'UTF8',
            PARSER_VERSION = '1.0'         
        )
    );


-- Create an external file format with tab for field terminator (tsv files)
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'tsv_file_format')
    CREATE EXTERNAL FILE FORMAT tsv_file_format
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,  -- Specify the format type (e.g., DELIMITEDTEXT, PARQUET, etc.)
        FORMAT_OPTIONS (
            STRING_DELIMITER = '"',       -- Specify the string delimiter for text
            FIELD_TERMINATOR = '\t',       -- Specify the field terminator for delimited text
            FIRST_ROW = 2  ,              -- Specify the first row to read (if headers are present)
            USE_TYPE_DEFAULT = FALSE,
            ENCODING = 'UTF8',
            PARSER_VERSION = '2.0'         
        )
    );



IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_file_format')
    CREATE EXTERNAL FILE FORMAT parquet_file_format
    WITH (
            FORMAT_TYPE = PARQUET,
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
        );


IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'delta_file_format')
    CREATE EXTERNAL FILE FORMAT delta_file_format
    WITH (
            FORMAT_TYPE = DELTA,
            DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'          
        );

