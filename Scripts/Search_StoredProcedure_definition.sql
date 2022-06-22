CREATE TABLE #StoredProceduresTable (
	DatabaseName sysname,
	Name NVARCHAR(128),
	Definition NVARCHAR(MAX)
);

DECLARE @dbName sysname


DECLARE cursor_Databases CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
SELECT name
FROM master.dbo.sysdatabases
WHERE dbid > 4
	AND name NOT IN ( 'DQS_PROJECTS', 'SSISDB', 'DataMigrationAssistant', 'DQS_MAIN' );

OPEN cursor_Databases;

FETCH NEXT FROM cursor_Databases
INTO @dbName

WHILE (@@fetch_status <> -1)
BEGIN

	DECLARE @sql NVARCHAR(MAX);

	SET @sql
	= N'
USE '+ @dbName +
	' 
    SELECT
    DB_NAME() AS DatabaseName,
    OBJECT_NAME(m.OBJECT_ID) AS Name,
    m.definition
    FROM sys.objects o
    INNER JOIN sys.sql_modules m ON m.object_id = o.object_id
    
    WHERE
    o.type = ''P'''

	BEGIN TRY

	INSERT #StoredProceduresTable
	EXEC sp_executesql @sql;
	END TRY
	BEGIN CATCH
	PRINT ERROR_MESSAGE();
	END CATCH;

	FETCH NEXT FROM cursor_Databases
	INTO @dbName

END;

CLOSE cursor_Databases;
DEALLOCATE cursor_Databases;


SELECT DatabaseName,
	   Name,
	   Definition
FROM #StoredProceduresTable
--WHERE Definition LIKE '%HumanResources%'

IF (OBJECT_ID('tempdb..#StoredProceduresTable','u') IS NOT NULL)
	DROP TABLE #StoredProceduresTable

