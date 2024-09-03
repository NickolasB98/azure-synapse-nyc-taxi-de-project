SELECT
    MAX(len(LocationID)) AS len_locationId,
    MAX(len(Borough)) AS len_borough,
    MAX(len(Zone)) AS len_zone,
    MAX(len(service_zone)) AS len_service_zone
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]



-- use WITH clause to provide explicit data types
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )
    AS [result]

-- change the collation of the whole database (created the nyc taxi database for the project)
select name,collation_name from sys.databases;
-- find in data -> sql database
create database nyc_taxi_discovery;
alter database nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;




-- select a subset of columns
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        Zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
    AS [result]




-- fix column names !put the numbers as assigned in the columns 1,2,3,4 if we change
-- these numbers we can put the columns in any way we like them
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw/taxi_zone.csv',
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
    AS [result]

-- create external data source to stop using the whole https or abfss file endpoint
-- create the data source on the container of our storage account
-- CANT CREATE EXTERNAL DATA SOURCE IN THE MASTER DB
-- this data source is found in Data -> Workspace -> SQL Database -> nyc_taxi_discovery
-- -> external resources -> external data sources
CREATE EXTERNAL DATA SOURCE nyc_taxi_data
WITH (
    LOCATION = 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/'
)

-- now we can access our whole raw folder by using this external data source!!
-- this data source has every folder in the container
-- if we put the silver in the future it will also access it

-- lets create one data source for only the raw data
CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
WITH (
    LOCATION = 'abfss://nyc-taxi-data@synapsecoursedlnikolas.dfs.core.windows.net/raw'
)

-- same sql script with the external data source

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'raw/taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data',
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
    AS [result]


-- use the new external data source for the raw data only 

SELECT
    TOP 100 *
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


-- drop the external data source that is not specified for bronze,silver,gold
-- we follow the bronze,silver,gold architecture data sources 


drop external data source nyc_taxi_data;


--where is my data source pointing at?
select name, location from sys.external_data_sources;

