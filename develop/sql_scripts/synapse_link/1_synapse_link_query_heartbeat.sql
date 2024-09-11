IF (NOT EXISTS(SELECT * FROM sys.credentials WHERE name = 'synapse-course-cosmos-db-nikolas'))
    CREATE CREDENTIAL [synapse-course-cosmos-db-nikolas]    WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ''

GO

SELECT TOP 100 *
FROM OPENROWSET(​PROVIDER = 'CosmosDB',
                CONNECTION = 'Account=synapse-course-cosmos-db-nikolas;Database=nyctaxidb',
                OBJECT = 'Heartbeat',
                SERVER_CREDENTIAL = 'synapse-course-cosmos-db-nikolas'
) AS [Heartbeat]
