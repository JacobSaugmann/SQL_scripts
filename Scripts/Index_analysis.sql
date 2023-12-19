SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON;
--USE [DatabaseName]
--GO


DECLARE @db_id SMALLINT = DB_ID()
DECLARE @min_advantage TINYINT = 80
DECLARE @getselectability BIT = 0
DECLARE @drop_tmp_table BIT = 0
DECLARE @meta_age INT = -1
DECLARE @only_index_analysis BIT = 1
DECLARE @limit_to_tablename NVARCHAR(512) = NULL --N'Accounts'


IF OBJECT_ID('tempdb..#MissingIndexes','U') IS NOT NULL
	DROP TABLE #MissingIndexes

IF OBJECT_ID('tempdb..#ExistingIndexes','U') IS NOT NULL
	DROP TABLE #ExistingIndexes

IF OBJECT_ID('tempdb..#useless_space_consumption','U') IS NOT NULL
	DROP TABLE #useless_space_consumption

--Getting metadata age
SELECT @meta_age = CASE WHEN DATEDIFF(DAY, d.create_date, GETDATE()) = 0 THEN 1
	                                                                     ELSE DATEDIFF(DAY, d.create_date, GETDATE()) END
FROM sys.databases d
WHERE d.name = 'tempdb'

IF @meta_age < 14
BEGIN
	DECLARE @msg NVARCHAR(200) = N'Warning! metadata is only '+CAST(@meta_age AS NVARCHAR(3))+ ' days old, the data should be at least 14 days old for at more precise result'
	RAISERROR (@msg, 0, 1) WITH NOWAIT
END

SELECT md.statement AS table_name,
	   'Missing_Index' AS table_type_desc,
	   md.equality_columns,
	   md.inequality_columns,
	   md.included_columns,
	   mgs.avg_total_user_cost,
	   mgs.avg_user_impact,
	   mgs.user_scans,
	   mgs.user_seeks,
	   mgs.last_user_scan,
	   mgs.last_user_seek,
	   @meta_age meta_data_age,
	   CONCAT('CREATE NONCLUSTERED INDEX IDX_',REPLACE(REPLACE(REPLACE(SUBSTRING(md.statement, CHARINDEX('.', md.statement)+1, LEN(md.statement)- LEN(CHARINDEX('.', md.statement))), '[',''), ']',''),'.','_'),'_',REPLACE(REPLACE(REPLACE(REPLACE(IIF(md.equality_columns IS NOT NULL, md.equality_columns, md.inequality_columns), '.','_'),'[',''),']',''),', ',''),' ON ',md.statement, ' (',IIF(md.equality_columns IS NOT NULL, md.equality_columns, md.inequality_columns) ,')', IIF(md.included_columns IS NOT NULL,' INCLUDE(' +md.included_columns+ ')', ''), ' WITH (DATA_COMPRESSION=PAGE);'   ) AS create_ix_stmt
	INTO #MissingIndexes
FROM sys.dm_db_missing_index_details md               
	INNER JOIN sys.dm_db_missing_index_groups mg      
			ON mg.index_handle = md.index_handle
	INNER JOIN sys.dm_db_missing_index_group_stats mgs
			ON mg.index_group_handle = mgs.group_handle

WHERE md.database_id = @db_id
	AND (
		mgs.avg_user_impact > @min_advantage
		OR mgs.avg_total_user_cost > 50)
ORDER BY md.object_id , mgs.avg_user_impact DESC


SELECT CONCAT(QUOTENAME(DB_NAME(@db_id)),'.', QUOTENAME(s.name),'.',QUOTENAME(t.name)) table_name,
	   'Existing_Index' table_type_desc,
	   i.name,
	   i.type_desc,
	   i.is_unique,
	   i.is_primary_key,
	   i.has_filter,
	   DS1.index_columns_names,
	   DS2.included_columns_names,
	   ids.record_count,
	   ids.fragment_count,
	   ids.avg_fragmentation_in_percent,
	   ius.user_lookups,
	   ius.user_scans,
	   ius.user_seeks,
	   ius.user_updates,
	   ius.last_user_update,
	   @meta_age meta_data_age
	INTO #ExistingIndexes
FROM sys.indexes i                                                                                
	CROSS APPLY sys.dm_db_index_physical_stats(@db_id, i.object_id, i.index_id,NULL,'SAMPLED') ids
	INNER JOIN sys.dm_db_index_usage_stats ius                                                    
			ON ius.index_id = ids.index_id
				AND ius.object_id = ids.object_id
				AND ius.database_id = ids.database_id
	INNER JOIN sys.tables t                                                                       
			ON t.object_id = ids.object_id
	INNER JOIN sys.schemas s                                                                      
			ON s.schema_id = t.schema_id
	CROSS APPLY (
		SELECT STUFF((
        	SELECT ' [' + CLS.[name] + '];'
            FROM [sys].[index_columns] INXCLS 
            	INNER JOIN [sys].[columns] CLS
            			ON INXCLS.[object_id] = CLS.[object_id]
            				AND INXCLS.[column_id] = CLS.[column_id]
            WHERE i.[object_id] = INXCLS.[object_id]
            	AND i.[index_id] = INXCLS.[index_id]
            	AND INXCLS.[is_included_column] = 0
            FOR XML PATH('')
        	), 1, 1, '')
		) DS1([index_columns_names])                                                              
	CROSS APPLY (
		SELECT STUFF((
        	SELECT ' [' + CLS.[name] + '];'
            FROM [sys].[index_columns] INXCLS 
            	INNER JOIN [sys].[columns] CLS
            			ON INXCLS.[object_id] = CLS.[object_id]
            				AND INXCLS.[column_id] = CLS.[column_id]
            WHERE i.[object_id] = INXCLS.[object_id]
            	AND i.[index_id] = INXCLS.[index_id]
            	AND INXCLS.[is_included_column] = 1
            FOR XML PATH('')
        	), 1, 1, '')
		) DS2([included_columns_names])                                                           
WHERE i.type IN (1,2,5)
	AND ids.database_id = @db_id
	AND alloc_unit_type_desc = 'IN_ROW_DATA'
	AND i.is_disabled = 0

--Missing index on tables with existing index

;WITH INDEX_INFO
AS
(
	SELECT m.table_name COLLATE DATABASE_DEFAULT table_name,
    	   m.table_type_desc COLLATE DATABASE_DEFAULT table_type_desc,
    	   NULL AS index_name,
    	   NULL AS index_type_desc,
    	   NULL AS is_unique,
    	   NULL AS is_primary_key,
    	   NULL AS has_filter,
    	   m.equality_columns COLLATE DATABASE_DEFAULT index_columns_names,
    	   m.included_columns COLLATE DATABASE_DEFAULT included_columns,
    	   m.inequality_columns COLLATE DATABASE_DEFAULT inequality_columns,
    	   m.user_scans,
    	   m.user_seeks,
    	   NULL AS user_updates,
    	   CAST(m.avg_user_impact AS DECIMAL(18,2)) AS avg_user_impact,
    	   CAST(m.avg_total_user_cost AS DECIMAL(18,2))  AS avg_total_user_cost,
    	   CAST((m.avg_total_user_cost * (m.avg_user_impact /100.0) * m.user_seeks) / meta_data_age AS DECIMAL(18,2)) create_index_adv,
    	   m.meta_data_age,
		   m.create_ix_stmt AS create_index_statement
    FROM #MissingIndexes m
	UNION ALL
	SELECT DISTINCT e.table_name,
    	            e.table_type_desc,
    	            e.name,
    	            e.type_desc index_type_desc,
    	            e.is_unique,
    	            e.is_primary_key,
    	            e.has_filter,
    	            e.index_columns_names,
    	            e.included_columns_names,
    	            NULL AS inequality_columns,
    	            e.user_scans,
    	            e.user_seeks,
    	            e.user_updates,
    	            NULL AS avg_user_impact,
    	            NULL AS avg_total_user_cost,
    	            NULL AS create_index_adv_pr_day,
    	            meta_data_age,
					NULL AS create_index_statement
    FROM #ExistingIndexes e
)
SELECT *
FROM index_info
WHERE (
		@limit_to_tablename IS NULL
		OR table_name LIKE '%'+@limit_to_tablename+'%')
ORDER BY table_name , table_type_desc COLLATE DATABASE_DEFAULT

IF @only_index_analysis = 1
BEGIN
	GOTO CLEANUP
END

/* Get overlapping indexes */
;WITH OVERLAPPING_INDEXES
AS
(
	SELECT e.table_name,
    	   e.name AS index_name,
    	   e.index_columns_names,
    	   e.included_columns_names,
    	   CASE WHEN COALESCE(es.included_columns_names, '') = COALESCE(e.included_columns_names, '') THEN 'Overlapping'
    	                                                                                              ELSE 'Nearby overlapping' END AS index_state,
    	   ROW_NUMBER() OVER (PARTITION BY e.index_columns_names, e.table_name ORDER BY e.name) AS row_no
    FROM #ExistingIndexes e           
    	INNER JOIN #ExistingIndexes es
    			ON es.index_columns_names = e.index_columns_names
    				AND es.table_name = e.table_name
    				AND es.name <> e.name
    WHERE (
    		@limit_to_tablename IS NULL
    		OR e.table_name LIKE '%'+@limit_to_tablename+'%')
)
SELECT DISTINCT CASE WHEN oi.row_no > 1
		AND index_state = 'Overlapping'        THEN '[WARNING] overlapping index'
	                 WHEN oi.row_no > 1
		AND index_state = 'Nearby overlapping' THEN '[INFO] nearby overlapping index'
	                                           ELSE NULL END AS msg,
	            oi.table_name,
	            oi.index_name,
	            oi.index_columns_names,
	            oi.included_columns_names,
	            CASE WHEN oi.row_no > 1 THEN CONCAT('ALTER INDEX ', QUOTENAME(oi.index_name) ,' ON ',oi.table_name, ' DISABLE;') END AS disable_stmt
FROM OVERLAPPING_INDEXES oi
ORDER BY oi.table_name , oi.index_name




IF @meta_age < 100
BEGIN
	SET @msg = N'Warning! metadata is only '+CAST(@meta_age AS NVARCHAR(3))+ ' days old, remember some indexes may only be used every 3, 6 months but can have a significant performance impact on those queries'
	RAISERROR (@msg, 0, 1) WITH NOWAIT
END

/* Create Drop index statement */
SELECT  CONCAT('DROP INDEX ', QUOTENAME(e.name), ' ON ', e.table_name) AS drop_statement,
        CONCAT('ALTER INDEX ', QUOTENAME(e.name) ,' ON ',e.table_name, ' DISABLE;') AS disable_statement,
        e.user_lookups,
	e.user_scans,
	e.user_seeks,
	e.user_updates,
	e.meta_data_age
FROM #ExistingIndexes e
WHERE (((IIF(e.user_updates=0,1.0,e.user_updates*1.0) - (e.user_scans + e.user_seeks+ e.user_lookups))/IIF(e.user_updates=0,1,e.user_updates)) * 100 > 95 )
	--Don't show Unique constraint index and Promary key index
	AND e.is_primary_key = 0
	AND e.is_unique = 0
	AND (
		@limit_to_tablename IS NULL
		OR table_name LIKE '%'+@limit_to_tablename+'%')
ORDER BY e.table_name

/* Calculate Density and selectability */

IF @GetSelectability = 1
BEGIN

	RAISERROR ('Now calculating selectability, please be patient!', 0, 1) WITH NOWAIT

	SET NOCOUNT ON
	IF OBJECT_ID('tempdb..#Density_Selectability','U') IS NOT NULL
		DROP TABLE #Density_Selectability

	CREATE TABLE #Density_Selectability (
		table_name      SYSNAME ,
		column_name     SYSNAME ,
		row_count       BIGINT DEFAULT(1) ,
		column_density  DECIMAL(14,12) ,
		selectabiliry   DECIMAL(14,12) ,
		occurrences     INT ,
		max_lenght_byte INT NULL ,
		count_destinct  BIGINT NULL
	)
	/* Cursor  */
	DECLARE @tableName SYSNAME
	DECLARE @columnName SYSNAME
	DECLARE @rowcount BIGINT
	DECLARE @occurrences INT

	DECLARE density_cursor CURSOR FOR
	WITH ROW_COUNT
	AS
	(
		SELECT CONCAT(QUOTENAME(DB_NAME(DB_ID())),'.',QUOTENAME(SCHEMA_NAME(schema_id)),'.',QUOTENAME([Tables].name)) AS [TableName],
        	   SUM([Partitions].[rows]) AS [TotalRowCount]
        FROM sys.tables AS [Tables]                  
        	INNER JOIN sys.partitions AS [Partitions]
        			ON [Tables].[object_id] = [Partitions].[object_id]
        				AND [Partitions].index_id IN (0, 1)
        				AND (
        					@limit_to_tablename IS NULL
        					OR [Tables].name LIKE '%'+@limit_to_tablename+'%')
        -- WHERE [Tables].name = N'name of the table'
        GROUP BY SCHEMA_NAME(schema_id) , [Tables].name
	)
	SELECT DISTINCT missing_index_tmp.table_name,
    	            missing_index_tmp.index_column_name,
    	            CASE WHEN TotalRowCount = 0 THEN 1
    	                                        ELSE TotalRowCount END AS TotalRowCount,
    	            missing_index_tmp.occurrences
    FROM (
    SELECT tmp.table_name,
    	   tmp.index_column_name,
    	   tmp.TotalRowCount,
    	   COUNT(*) OVER (PARTITION BY tmp.table_name, tmp.index_column_name) occurrences
    FROM (
    SELECT table_name,
    	   LTRIM(RTRIM(m.n.value('.[1]','nvarchar(128)'))) AS index_column_name,
    	   r.TotalRowCount
    FROM (
    SELECT table_name,
    	   CAST('<XMLRoot><RowData>' + REPLACE(e.equality_columns,',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
    FROM #MissingIndexes e
    )t                                             
    	LEFT JOIN row_count r                      
    			ON t.table_name = r.TableName COLLATE DATABASE_DEFAULT
    	CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)) tmp
    WHERE index_column_name <> N''
    ) missing_index_tmp
    OPTION(RECOMPILE)

	OPEN density_cursor
	FETCH NEXT FROM density_cursor INTO @tableName, @columnName, @rowcount, @occurrences

	WHILE @@FETCH_STATUS = 0
	BEGIN
		/* Add Dynamic SQL here */

		DECLARE @statement NVARCHAR(2000) =
		'
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

        ;WITH Column_density
        AS
        (
        SELECT 1.0/ COUNT( DISTINCT '+ @ColumnName + ') column_density,
               COUNT( DISTINCT '+ @ColumnName + ') count_destinct
        FROM '+ @tableName+'
        )
        SELECT ''' +
		@tableName +''' table_name,'''+
		@columnName +''' column_name,'+'
            '+CAST(@rowcount AS VARCHAR(25)) +' row_count,
            d.column_density,
            d.column_density/'+CAST(@rowcount AS VARCHAR(25)) + ' selectability,
            '+CAST(@occurrences AS VARCHAR(5)) + ' occurrences,
            c.max_length,
            d.count_destinct
        FROM Column_density d
            INNER JOIN (
                SELECT c.max_length, QUOTENAME(c.name) column_name       
                FROM sys.columns c
                WHERE c.object_id = OBJECT_ID('''+@tableName+''', ''U'')
            ) c ON c.column_name = '''+@columnName+''''

		--PRINT @statement
		BEGIN TRY
		INSERT INTO #Density_Selectability
		EXEC sp_executesql @statement
		END TRY
		BEGIN CATCH
		DECLARE @ErrorMsg NVARCHAR(4000)

		SELECT @ErrorMsg = CONCAT('Error on ', @tableName, ' message: ')
		SELECT @ErrorMsg = @ErrorMsg + ERROR_MESSAGE()

		RAISERROR (@ErrorMsg, 0, 1) WITH NOWAIT

		END CATCH

		FETCH NEXT FROM density_cursor INTO @tableName, @columnName, @rowcount ,@occurrences
	END

	CLOSE density_cursor
	DEALLOCATE density_cursor


	SELECT *
    FROM #Density_Selectability
    ORDER BY table_name , column_name

	SET NOCOUNT OFF

END

/* Get waisted space by unused index */
;WITH DROP_INDEXES
AS
(
	SELECT e.name,
    	   e.record_count
    FROM #ExistingIndexes e
    WHERE (((IIF(e.user_updates=0,1.0,e.user_updates*1.0) - (e.user_scans + e.user_seeks+ e.user_lookups))/IIF(e.user_updates=0,1,e.user_updates)) * 100 > 95 )
    	AND e.is_primary_key = 0
    	AND e.is_unique = 0
)
SELECT i.object_id,
	   i.index_id,
	   di.record_count,
	   size.page_count,
	   size.mb_pages
	INTO #useless_space_consumption
FROM sys.indexes i            
	INNER JOIN drop_indexes di
			ON di.name = i.name COLLATE DATABASE_DEFAULT
	OUTER APPLY (
		SELECT COUNT(sz.used_page_count) AS page_count,
        	   SUM(sz.[used_page_count]) * 8/1024 AS mb_pages
        FROM sys.dm_db_partition_stats AS sz
        	INNER JOIN sys.indexes AS ix    
        			ON sz.[object_id] = ix.[object_id]
        				AND sz.[index_id] = ix.[index_id]
        	INNER JOIN sys.tables tn        
        			ON tn.OBJECT_ID = ix.object_id
        WHERE i.name = ix.[name]
        	AND i.object_id = ix.object_id
        	AND (
        		@limit_to_tablename IS NULL
        		OR tn.name LIKE '%'+@limit_to_tablename+'%')
        GROUP BY tn.[name] , ix.[name]
		) AS size             

IF (SELECT COALESCE(SUM(u.mb_pages),0)
    FROM #useless_space_consumption u ) > 0
BEGIN
	SELECT 'At least '+ CAST(COALESCE(SUM(u.mb_pages),0) AS VARCHAR(50)) + ' MB of waisted space' AS comment
    FROM #useless_space_consumption u

END
GOTO CLEANUP


CLEANUP:
BEGIN
IF @drop_tmp_table = 1
	BEGIN

		IF OBJECT_ID('tempdb..#MissingIndexes','U') IS NOT NULL
			DROP TABLE #MissingIndexes

		IF OBJECT_ID('tempdb..#ExistingIndexes','U') IS NOT NULL
			DROP TABLE #ExistingIndexes

		IF OBJECT_ID('tempdb..#Density_Selectability','U') IS NOT NULL
			DROP TABLE #Density_Selectability


		IF OBJECT_ID('tempdb..#useless_space_consumption','U') IS NOT NULL
			DROP TABLE #useless_space_consumption
	END
END
