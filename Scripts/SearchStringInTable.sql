/* Change database to the database to be searched for a string 
Created: 28-01-2019
By: Jacob Saugmann
Search and replace:
[DatabaseName] = The database you want to search in
*/

USE Demo
GO

IF(OBJECT_ID('spSearchStringInTable', 'P')) IS NOT NULL
    DROP PROCEDURE spSearchStringInTable
GO
  
CREATE PROCEDURE dbo.spSearchStringInTable(@SearchString NVARCHAR(MAX),
 @Table_Schema sysname = 'dbo',
 @Table_Name sysname,
 @IsCharSearch BIT = 0)
 WITH ENCRYPTION
 AS
 BEGIN
 

DECLARE @Columns NVARCHAR(MAX), @Cols NVARCHAR(MAX), @PkColumn NVARCHAR(MAX)
  
-- Get all character columns
 SET @Columns = STUFF((SELECT ', ' + QUOTENAME(Column_Name)
 FROM INFORMATION_SCHEMA.COLUMNS
 WHERE DATA_TYPE IN ('text','ntext','varchar','nvarchar','char','nchar')
 AND TABLE_NAME = @Table_Name AND TABLE_SCHEMA = @Table_Schema
 ORDER BY COLUMN_NAME
 FOR XML PATH('')),1,2,'');
  
IF @Columns IS NULL -- no character columns
   RETURN -1;
  
-- Get columns for select statement - we need to convert all columns to nvarchar(max)
SET @Cols = STUFF((SELECT ', CAST(' + QUOTENAME(Column_Name) + ' AS nvarchar(max)) COLLATE DATABASE_DEFAULT AS ' + QUOTENAME(Column_Name)
 FROM INFORMATION_SCHEMA.COLUMNS
 WHERE DATA_TYPE IN ('text','ntext','varchar','nvarchar','char','nchar')
 AND TABLE_NAME = @Table_Name AND TABLE_SCHEMA = @Table_Schema
 ORDER BY COLUMN_NAME
 FOR XML PATH('')),1,2,'');
   
 SET @PkColumn = STUFF((SELECT N' + ''|'' + ' + ' CAST(' + QUOTENAME(CU.COLUMN_NAME) + ' AS nvarchar(max)) COLLATE DATABASE_DEFAULT '
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
INNER JOIN
INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS CU
ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
TC.CONSTRAINT_NAME = CU.CONSTRAINT_NAME COLLATE DATABASE_DEFAULT
  
 WHERE TC.TABLE_SCHEMA = @Table_Schema AND TC.TABLE_NAME = @Table_Name
 ORDER BY CU.ORDINAL_POSITION
 FOR XML PATH('')),1,9,'');
  
 IF @PkColumn IS NULL
    SELECT @PkColumn = 'CAST(NULL AS nvarchar(max))';
      
 -- set select statement using dynamic UNPIVOT
 DECLARE @SQL NVARCHAR(MAX)
 IF(@IsCharSearch = 1)
 BEGIN
 SET @SQL = 'SELECT *, ' + QUOTENAME(@Table_Schema,'''') + ' AS [Table Schema], ' + QUOTENAME(@Table_Name,'''') + ' AS [Table Name]' +
  ' FROM
  (SELECT '+ @PkColumn + ' AS [PK Column], ' + @Cols + ' FROM ' + QUOTENAME(@Table_Schema) + '.' + QUOTENAME(@Table_Name) +  ' ) src UNPIVOT ([Column Value] for [Column Name] IN (' + @Columns + ')) unpvt
 WHERE [Column Value] LIKE ''%' +@SearchString + '%'''
 END
 ELSE
 BEGIN
 SET @SQL = 'SELECT *, ' + QUOTENAME(@Table_Schema,'''') + ' AS [Table Schema], ' + QUOTENAME(@Table_Name,'''') + ' AS [Table Name]' +
  ' FROM
  (SELECT '+ @PkColumn + ' AS [PK Column], ' + @Cols + ' FROM ' + QUOTENAME(@Table_Schema) + '.' + QUOTENAME(@Table_Name) +  ' ) src UNPIVOT ([Column Value] for [Column Name] IN (' + @Columns + ')) unpvt
 WHERE [Column Value] LIKE ''%''+' +REPLACE(@SearchString, '''','') + '+''%'''

 END

print @SQL
  
EXECUTE sp_ExecuteSQL @SQL, N'@SearchString nvarchar(max)', @SearchString;
END
GO

/* Creates a log table inside master */

 USE master
 GO

 IF (OBJECT_ID( 'dbo.SearchTablesLog', 'u')) IS NOT NULL
    DROP TABLE master.dbo.SearchTablesLog

 CREATE TABLE master.dbo.SearchTablesLog
 (
    Id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
    Table_Schema SYSNAME,
    Table_Name SYSNAME,
    Search_started DATETIME2,
    Search_Completed DATETIME2,
    Has_Error BIT DEFAULT 0,
    Error_Message NVARCHAR(4000) 
 )
 GO

 
 IF (OBJECT_ID( 'dbo.SearchTablesResults', 'u')) IS NOT NULL
    DROP TABLE master.dbo.SearchTablesResults

 CREATE TABLE master.dbo.SearchTablesResults
 (
    [PK COLUMN] NVARCHAR(MAX), 
    [COLUMN VALUE] NVARCHAR(MAX), 
    [COLUMN Name] sysname, 
    [TABLE SCHEMA] sysname, 
    [TABLE Name] sysname
    
 )
 GO
 
/* Change database to the database to be searched for a string */
 USE Demo
 GO

INSERT INTO master.dbo.SearchTablesLog (Table_Schema, Table_Name)
SELECT   Table_Schema, Table_Name
FROM     INFORMATION_SCHEMA.Tables   
WHERE TABLE_TYPE = 'BASE TABLE' 
ORDER BY Table_Schema, Table_Name

GO

/* DEBUG */
--SELECT * FROM master.dbo.SearchTablesLog

IF(OBJECT_ID('SearchTablesForString', 'P')) IS NOT NULL
    DROP PROC SearchTablesForString
GO

CREATE PROC SearchTablesForString(@Take INT = 5,@SearchString NVARCHAR(MAX), @IsCharSearch BIT = 0 )
WITH ENCRYPTION
AS
BEGIN
DECLARE @Table_Name sysname, @Table_Schema sysname, @id INT

IF(OBJECT_ID('tempdb..##TablesToBeSearched', 'U')) IS NOT NULL
DROP TABLE #TablesToBeSearched

;With TheNextTables_CTE
AS(
SELECT row_number() OVER(order by id asc) RowNo, Id, Table_Schema, Table_Name
FROM master.dbo.SearchTablesLog L
WHERE L.Search_Completed IS NULL AND Has_Error <> 1
)
SELECT Id, Table_Schema, Table_Name
INTO #TablesToBeSearched
FROM TheNextTables_CTE
WHERE RowNo <= @take

DECLARE curAllTables CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY
    FOR
      SELECT  Id, Table_Schema, Table_Name
      FROM #TablesToBeSearched     
   
     
    OPEN curAllTables
    FETCH  curAllTables
    INTO @id, @Table_Schema, @Table_Name   
    WHILE (@@FETCH_STATUS = 0) -- Loop through all tables in the database
      BEGIN
        UPDATE L
            SET Search_started = GETDATE()
        FROM master.dbo.SearchTablesLog L
        WHERE l.Id = @Id
        

BEGIN TRY
        INSERT master.dbo.SearchTablesResults ([PK COLUMN], [Column Value], [Column Name], [Table Schema], [Table Name])
        EXECUTE spSearchStringInTable @SearchString, @Table_Schema, @Table_Name, @IsCharSearch
     
        UPDATE L
            SET Search_Completed = GETDATE()
        FROM master.dbo.SearchTablesLog L
        WHERE l.Id = @Id

END TRY   
BEGIN CATCH
/* The execution has error log error message */

        UPDATE L
            SET Search_Completed = null,
                Has_Error = 1,
                Error_Message = ERROR_MESSAGE()
        FROM master.dbo.SearchTablesLog L
        WHERE l.Id = @Id
END CATCH
       
        FETCH  curAllTables
        INTO @id,@Table_Schema, @Table_Name
      END -- while
    CLOSE curAllTables
    DEALLOCATE curAllTables
 END
 GO

  /*DEBUG*/
 --DECLARE @SearchString NvARCHAR(MAX) = 'CHAR(13)'
 --EXEC SearchTablesForString 10, @SearchString, 1

-- ;WITH MyTable_CTE
-- AS(
-- SELECT    
--     [COLUMN VALUE], 
--     [COLUMN Name], 
--     [TABLE SCHEMA], 
--     [TABLE Name]
-- FROM [master].[dbo].[SearchTablesResults]
-- GROUP BY [COLUMN VALUE],[COLUMN Name],[TABLE SCHEMA],[TABLE Name]
-- )
-- SELECT     
--     [COLUMN VALUE], 
--     [COLUMN Name], 
--     [TABLE SCHEMA], 
--     [TABLE Name],
--     CONCAT('UPDATE T ', 'SET ',  QUOTENAME([COLUMN Name]) , '=REPLACE(',QUOTENAME([COLUMN Name]),',',@SearchString, ','''')', ' FROM ',QUOTENAME([TABLE SCHEMA]),'.' ,QUOTENAME([TABLE Name]) , ' AS T WHERE ',QUOTENAME([COLUMN Name]) ,' LIKE  ''%''+' +REPLACE(@SearchString, '''','') + '+''%''') [LetsMakeItRight]
-- FROM MyTable_CTE

