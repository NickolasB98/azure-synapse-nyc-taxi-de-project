-- create the external data source 

USE nyc_taxi_ldw;

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'nyc_taxi_src')
    CREATE EXTERNAL DATA SOURCE nyc_taxi_src
    WITH (
        LOCATION = 'https://synapsecoursedlnikolas.dfs.core.windows.net/nyc-taxi-data'
    );