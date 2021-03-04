
DECLARE @path_to_excel NVARCHAR(256) = 'C:\\Temp\\Index analyse_olt.xlsx'
DECLARE @index_adv_min INT = 100


DROP TABLE IF EXISTS #index

CREATE TABLE #index (
	table_name SYSNAME NULL,
	table_type_desc VARCHAR(20) NULL,
	index_name SYSNAME NULL,
	index_type_desc VARCHAR(30) NULL,
	is_unique BIT,
	is_primary_key BIT,
	has_filter BIT,
	index_columns_names VARCHAR(2000),
	included_columns VARCHAR(2000),
	inequality_columns VARCHAR(2000),
	user_scans VARCHAR(30),
	user_seeks VARCHAR(30),
	user_updates VARCHAR(30),
	avg_user_impact VARCHAR(30),
	avg_total_user_cost VARCHAR(30),
	create_index_adv VARCHAR(30),
	meta_data_age INT
)


INSERT INTO #index ( table_name, table_type_desc, index_name, index_type_desc, is_unique, is_primary_key, has_filter, index_columns_names, included_columns, inequality_columns, user_scans, user_seeks, user_updates, avg_user_impact, avg_total_user_cost, create_index_adv, meta_data_age )
--DECLARE @path_to_excel NVARCHAR(256) = 'C:\\Temp\\Index analyse_olt.xlsx'
EXEC sp_execute_external_script
@language = N'Python',
@script = N'import pandas as pd
import numpy as np

df = pd.read_excel(path, keep_default_na=False, dtype={"table_name":str, 
                                                       "table_type_desc":str,
                                                       "index_name":str,
                                                       "index_type_desc":str,
                                                       "is_unique":bool,
                                                       "is_primary_key":bool,
                                                       "has_filter":bool,
                                                       "index_columns_names":str,
                                                       "included_columns ":str,
                                                       "inequality_columns":str,
                                                       "user_scans":str,
                                                       "user_seeks":str,
                                                       "user_updates":str,
                                                       "avg_user_impact":str,
                                                       "avg_total_user_cost":str,
                                                       "create_index_adv":str,
                                                       "meta_data_age":int})
OutputDataSet = df',
@params = N'@path NVARCHAR(256)',
@path = @path_to_excel


SET NOCOUNT ON;

DECLARE @sql NVARCHAR(2000)
DECLARE datafix CURSOR
FOR(
SELECT CONCAT('UPDATE #index', ' SET ', cn.name , ' = NULL WHERE ', cn.name, '=''NULL''') as sqlcmd
FROM   tempdb.sys.columns cn
	INNER JOIN tempdb.sys.types t
    ON t.system_type_id = cn.system_type_id
WHERE  object_id = Object_id('tempdb..#index')
AND t.name IN (N'varchar', N'nvarchar', N'sysname')
) 

OPEN datafix
FETCH NEXT FROM datafix INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN

	EXEC sp_executesql @sql
	FETCH NEXT FROM datafix INTO @sql
END

CLOSE datafix
DEALLOCATE datafix

DROP TABLE IF EXISTS #prep_indexes

;WITH MISSING_INDEXES
AS
(
	SELECT table_name,
    	   table_type_desc,
    	   index_name,
    	   index_type_desc,
    	   is_unique,
    	   is_primary_key,
    	   has_filter,
    	   index_columns_names,
    	   included_columns,
    	   inequality_columns,
    	   CAST (user_scans AS BIGINT) AS user_scans,
    	   CAST(user_seeks AS BIGINT) AS user_seeks,
    	   CAST(avg_user_impact AS DECIMAL(8,2)) AS avg_user_impact,
    	   CAST(avg_total_user_cost AS DECIMAL(8,2)) AS avg_total_user_cost,
    	   CAST(create_index_adv AS DECIMAL(8,2)) AS create_index_adv,
    	   meta_data_age
    FROM #index
    WHERE table_type_desc = 'Missing_Index'
),    UPDATES_ON_TABLE
AS
(
	SELECT CAST(MAX(user_updates) AS BIGINT) AS user_updates,
    	   table_name
    FROM #index
    GROUP BY table_name

)
SELECT i.table_name,
	   table_type_desc,	 
	   index_columns_names,
	   included_columns,
	   inequality_columns,
	   user_scans,
	   user_seeks,
	   ut.user_updates AS table_updates,
	   avg_user_impact,
	   avg_total_user_cost,
	   create_index_adv,
	   meta_data_age
INTO #prep_indexes
FROM missing_indexes i           
	LEFT JOIN UPDATES_ON_TABLE ut
			ON i.table_name = ut.table_name
WHERE create_index_adv >= @index_adv_min

;WITH prep_statements
AS(
SELECT 
	CASE WHEN NULLIF(p.inequality_columns, '') IS NOT NULL THEN CONCAT(p.inequality_columns, ', ', p.index_columns_names)
		 ELSE p.index_columns_names END AS index_keys,
	CASE WHEN p.included_columns IS NOT NULL THEN CONCAT('INCLUDE( ', p.included_columns, ' )')		
	ELSE '' END AS included_columns,	
	REVERSE(SUBSTRING(REVERSE(p.table_name),0,CHARINDEX('.',REVERSE(p.table_name)))) AS index_name,
	table_name,
	user_scans,
	user_seeks,
	table_updates,
	create_index_adv
FROM #prep_indexes p
)
SELECT ps.table_name,
	ps.user_scans,
	ps.user_seeks,
	ps.table_updates,
	ps.create_index_adv,
	CONCAT('CREATE NONCLUSTERED INDEX idx_',LOWER(REPLACE(REPLACE(ps.index_name, '[', ''), ']','')),'_',LOWER(REPLACE(REPLACE(REPLACE(ps.index_keys, ', ', '_'), '[', ''), ']', '')),' ON ', ps.table_name, ' (', ps.index_keys, ')',ps.included_columns, ' WITH (DATA_COMPRESSION=PAGE)') AS create_indes_stmt
FROM prep_statements ps
ORDER BY create_index_adv DESC, user_seeks DESC

DECLARE @metaage BIGINT, @msg NVARCHAR(500)
SELECT TOP 1 @metaage = meta_data_age FROM #index

IF @metaage < 15
BEGIN
	SET @msg = CONCAT('WARNING! the server have not run for more than ', @metaage,' days, use the create index statements with caution!')
	RAISERROR(@msg,10,0) WITH NOWAIT
END
ELSE
BEGIN
	SET @msg = CONCAT('INFO! the server have run for ', @metaage,' days!')
	RAISERROR(@msg,10,0) WITH NOWAIT
	
END


DROP TABLE IF EXISTS #index
DROP TABLE IF EXISTS #prep_indexes
