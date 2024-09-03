-- to create oour first external table 
-- we need to create 

-- a logical data warehouse (ldw) database (nyc_taxi_ldw)
-- an external data source (nyc_taxi_src)
-- specify the file format (csv_file_format) in this case 
-- the format is reuusable for other csvs
-- the database schema, in this scenario we are in bronze schema


-- we need to use the master database to create the ldw database

USE master
GO


CREATE DATABASE nyc_taxi_ldw
GO

-- we want to alter the database to use the UTF-8 collation 
-- because we got strings 
-- recommended for serverless sql is Latin1_General_100_BIN2_UTF8


ALTER DATABASE nyc_taxi_ldw COLLATE Latin1_General_100_BIN2_UTF8
GO 
-- GO is used to seperate the code , makes them act as individual transactions

-- create the schema (all three of them)
-- we now switch to the database we created in order to create the schema
USE nyc_taxi_ldw
GO


CREATE SCHEMA bronze 
GO 

CREATE SCHEMA silver 
GO 

CREATE SCHEMA gold 
GO 




