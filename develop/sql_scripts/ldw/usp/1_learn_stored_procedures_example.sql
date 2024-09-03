-- STORED PROCEDURES

/* Definition:

A group of SQL statements stored as a named object in a database.
Executed as a single unit.
Key Features:

Input Parameters: Accept input values.
Output Parameters: Can return output values.
Reusability: Can be called multiple times.
Encapsulation: Group related SQL statements into a single unit.
Maintenance: Easier to update and maintain.
Security: Can restrict access to underlying database objects.
Benefits:

Efficiency: Reduce code duplication.
Modularity: Improve code organization.
Maintainability: Centralize changes.
Performance: Potential performance improvements.
Security: Enforce access controls.
Limitations (in Synapse SQL):

Supported T-SQL: Ensure the T-SQL statements used within the stored procedure are supported by Synapse SQL.
Feature Limitations: Some features might have limitations compared to SQL Server.
Remember:

Use stored procedures for frequently executed SQL code blocks.
Consider performance implications when using complex stored procedures.
Follow best practices for writing efficient and maintainable stored procedures.
*/

--lets see one example
USE nyc_taxi_ldw;
GO

CREATE PROCEDURE usp_test
    @borough NVARCHAR(50)  -- Declare the parameter with its data type
AS
BEGIN
    SELECT *
    FROM bronze.taxi_zone
    WHERE borough = @borough;
END;

EXEC usp_test @borough = 'Queens'









