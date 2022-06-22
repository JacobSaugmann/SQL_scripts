DECLARE @tablename SYSNAME = 'Z', 
        @schemaname SYSNAME = 'dbo',
        @char_big NVARCHAR(8) = 'UNKNOWN',
        @char_small NVARCHAR(4) = 'UNKN',
        @char_tiny NVARCHAR(1) = '0',
        @date NVARCHAR(8) = '19000101',
        @number NVARCHAR(1) = '0'

IF(OBJECT_ID('tempdb..#myUpdateTable', 'U') IS NOT NULL)
    DROP TABLE #myUpdateTable

;WITH meta_data
AS(
SELECT 
       QUOTENAME(c.name) as column_name,
        CASE WHEN ty.name LIKE '%char%' THEN CONCAT(ty.name,'(',c.max_length,')') ELSE ty.name END AS datatype_lenght_txt,
       c.max_length,
       CASE WHEN ty.name LIKE '%char%' THEN 'char'
            ELSE ty.name
        END AS data_type,
        CONCAT(QUOTENAME(sc.name),'.' ,QUOTENAME(t.name)) AS table_name
FROM sys.columns c
    INNER JOIN sys.tables t
        ON c.object_id = t.object_id
    INNER JOIN sys.types ty
        ON ty.user_type_id = c.user_type_id
    INNER JOIN sys.schemas sc
        ON sc.schema_id = t.schema_id
WHERE t.name = @tablename AND sc.name = @schemaname AND c.is_nullable = 1 AND c.is_identity = 0)
SELECT CONCAT( 'UPDATE u ', 'SET ', column_name, ' = ', CASE WHEN data_type ='char' AND max_length >= 7 THEN ''''+@char_big+''''  
                                                        WHEN data_type ='char' AND max_length < 7 AND max_length > 3  THEN ''''+ @char_small +''''
                                                        WHEN data_type ='char' AND max_length <= 2  THEN ''''+@char_tiny+'''' 
                                                        WHEN data_type LIKE 'date%' THEN ''''+@date+'''' ELSE ''''+@number+'''' END
                                                        , ' FROM ', table_name,' AS u ' ,' WHERE ', column_name, ' IS NULL'  ) AS upd_stmt,
       CONCAT(' SELECT @row_count = COUNT(*) ', ' FROM ', table_name, ' WHERE ', column_name , ' IS NULL') AS test_stmt
INTO #myUpdateTable
FROM meta_data m;

DECLARE @test_stmt NVARCHAR(4000), @upd_stmt NVARCHAR(4000)
DECLARE upd_cursor CURSOR FORWARD_ONLY FOR 
    SELECT test_stmt, upd_stmt FROM #myUpdateTable 

OPEN upd_cursor
FETCH NEXT FROM upd_cursor INTO @test_stmt, @upd_stmt

WHILE @@FETCH_STATUS = 0
BEGIN
    

    DECLARE @ParmDefinition nvarchar(500), @rc BIGINT, @row_count BIGINT
    SET @ParmDefinition = N'@row_count BIGINT OUTPUT'
    EXEC sp_executesql @query = @test_stmt,
                       @ParmDefinition = @ParmDefinition, 
                       @row_count = @rc OUTPUT
    IF @rc > 1
    BEGIN
        /* Update if any rows match null */
    BEGIN TRY
        BEGIN TRANSACTION		

        PRINT @upd_stmt
        --Uncomment this to make the update!
        --EXEC sp_executesql @query = @upd_stmt
        
        COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE()
        ROLLBACK TRANSACTION
    END CATCH

    END

FETCH NEXT FROM upd_cursor INTO @test_stmt, @upd_stmt
END

CLOSE upd_cursor;
DEALLOCATE upd_cursor;


IF(OBJECT_ID('tempdb..#myUpdateTable', 'U') IS NOT NULL)
    DROP TABLE #myUpdateTable

