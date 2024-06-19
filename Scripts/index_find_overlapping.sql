
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON;
--USE [DatabaseName]
--GO


DECLARE @db_id SMALLINT = DB_ID()
DECLARE @min_advantage TINYINT = 80
DECLARE @getselectability BIT = 0
DECLARE @drop_tmp_table BIT = 0
DECLARE @meta_age INT = -1
DECLARE @only_index_analysis BIT = 0
DECLARE @limit_to_tablename VARCHAR(512) = ''
DECLARE @limit_to_indexname VARCHAR(512) = ''

IF OBJECT_ID('tempdb..#ExistingIndexes','U') IS NOT NULL
	DROP TABLE #ExistingIndexes

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
				AND i.name  LIKE '%'+@limit_to_indexname+'%'
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
				AND i.name  LIKE '%'+@limit_to_indexname+'%'
            FOR XML PATH('')
        	), 1, 1, '')
		) DS2([included_columns_names])                                                           
WHERE i.type IN (1,2,5)
	AND ids.database_id = @db_id
	AND alloc_unit_type_desc = 'IN_ROW_DATA'
	AND i.is_disabled = 0
	AND t.name LIKE '%'+@limit_to_tablename+'%'


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
WHERE oi.table_name LIKE '%'+@limit_to_tablename+'%'
	OR oi.index_name LIKE '%'+@limit_to_indexname+'%'
ORDER BY oi.table_name , oi.index_name
