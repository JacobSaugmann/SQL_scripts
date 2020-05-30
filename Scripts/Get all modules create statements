

SET NOCOUNT ON;

RAISERROR ('-| Documentation process started', 0, 1) WITH NOWAIT

IF (OBJECT_ID('tempdb..#lookup', 'U') IS NOT NULL)
	DROP TABLE #lookup

SELECT CONCAT(QUOTENAME(SCHEMA_NAME(o.schema_id)),'.', QUOTENAME(name)) AS fully_qualified_name,
	   o.create_date,
	   o.modify_date,
	   CASE o.type WHEN 'P'  THEN 'Stored_procedure'
	               WHEN 'TV' THEN 'Table_valued_function'
	               WHEN 'FN' THEN 'Scalar_function' 
                   WHEN 'SO' THEN  'Sequence_object' END AS type_description
	INTO #lookup
FROM sys.objects o
WHERE type IN ('P', 'TF','FN', 'SO')

IF (OBJECT_ID('tempdb..#params', 'U') IS NOT NULL)
	DROP TABLE #params

;WITH params
AS(
SELECT
	p.name AS sp_name,
	pa.name AS param_name,
	t.name AS t_name,
	SCHEMA_NAME(p.schema_id) AS schema_name
FROM sys.objects p
	INNER JOIN sys.parameters pa
		ON p.object_id = pa.object_id
	INNER JOIN sys.types t
		ON t.system_type_id = pa.system_type_id
)
SELECT DISTINCT 
	CONCAT(QUOTENAME(pa.schema_name),'.',QUOTENAME(pa.sp_name)) AS module_name,
(SELECT  CONCAT(REPLACE(p.param_name, '@',''), ' (',p.t_name, ') | ') AS module_params  
FROM params p
WHERE p.sp_name = pa.sp_name
FOR XML PATH ('')) param_names
INTO #params
FROM params pa


IF (OBJECT_ID('tempdb..#temp_module', 'U') IS NOT NULL)
	DROP TABLE #temp_module

CREATE TABLE #temp_module (
	name          VARCHAR(2000) ,
	params		  NVARCHAR(2000),
	create_date   VARCHAR(256) ,
	modified_date VARCHAR(256) ,
	type_desc     NVARCHAR(256) ,
	definition    NVARCHAR(MAX)
)


RAISERROR ('--| Starting cursor', 0, 1) WITH NOWAIT


DECLARE @name     SYSNAME ,
        @modified DATETIME2 ,
        @created  DATETIME ,
        @type     NVARCHAR(256),
		@params NVARCHAR(2000)

DECLARE module_cursor CURSOR
FOR SELECT l.fully_qualified_name,
    	   l.create_date,
    	   l.modify_date,
    	   l.type_description,
		   p.param_names
    FROM #lookup l
	LEFT JOIN #params p
		ON l.fully_qualified_name = p.module_name
   

OPEN module_cursor
FETCH NEXT FROM module_cursor INTO @name, @created, @modified, @type, @params

WHILE @@FETCH_STATUS = 0
BEGIN

	IF (OBJECT_ID('tempdb..#result', 'U') IS NOT NULL)
		DROP TABLE #result

	CREATE TABLE #result (
		line NVARCHAR(MAX)
	)

	BEGIN TRY

	DECLARE @csr_txt NVARCHAR(2000) = CONCAT('--| Getting declaration of', @name)
	RAISERROR (@csr_txt, 0, 1) WITH NOWAIT


	INSERT INTO #result
	EXEC sys.sp_helptext @name

	INSERT INTO #temp_module ( name,params ,create_date, modified_date, type_desc, definition )
	SELECT CONCAT('<strong>Name:</strong> ', CHAR(13), '<h2 style="font-family: verdana;color:#2f5596; font-weight:normal; font-size:16pt;">', @name , '</h2>' ),
		   CONCAT('<strong>Params:</strong> ', @params ),
    	   CONCAT('<strong>Created:</strong> ', CONVERT(VARCHAR(17), @created, 102)),
    	   CONCAT('<strong>Modified:</strong> ', CONVERT(VARCHAR(17), @modified, 102)),
    	   CONCAT('<strong>Type:</strong> ',@type ),
    	   ISNULL(STUFF((SELECT r.line + '<br/> '
                         FROM #result r
                         FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 0, ''), 'Error retrieving') AS d

   
	END TRY
	BEGIN CATCH
	DECLARE @msg NVARCHAR(2000) = CONCAT('--| ERROR: ', ERROR_MESSAGE())
	RAISERROR (@msg, 0, 1) WITH NOWAIT
	END CATCH

	FETCH NEXT FROM module_cursor INTO @name, @created, @modified, @type, @params

END

CLOSE module_cursor
DEALLOCATE module_cursor

IF (OBJECT_ID('dbo.documentation', 'U') IS NOT NULL)
	DROP TABLE dbo.documentation

;WITH prepared_modules_data
AS(
SELECT REPLACE(REPLACE(m.Name, '[', ''), ']', '') AS Name,
	   REPLACE(REPLACE(m.params, '</module_params>', ''),'<module_params>', '') AS params,
       m.create_date,
       m.modified_date,
       m.type_desc,
      SUBSTRING(m.definition, CHARINDEX('/*', m.definition), (CHARINDEX('*/', m.definition))-(CHARINDEX('/*', m.definition)) ) AS description,
      CONCAT( m.definition, CHAR(13)) AS definition,
      CHARINDEX('/*', m.definition) AS cmt_start,
      CHARINDEX('*/', m.definition) AS cmt_end
FROM #temp_module m
)
SELECT CONCAT('<p style="font-size: 11pt; font-family: verdana">',pmd.name, '</p>') AS name,
	   CONCAT('<p style="font-size: 11pt; font-family: verdana">', pmd.params , '<p>') AS params,
       CONCAT('<p  style="font-size: 11pt; font-family: verdana">',  pmd.create_date, '</p>') AS create_date,
       CONCAT('<p  style="font-size: 11pt; font-family: verdana">',  pmd.modified_date, '</p>') AS modified_date,
       CONCAT('<p  style="font-size: 11pt; font-family: verdana">',  pmd.type_desc, '</p>') AS type_desc,
       CONCAT('<p  style="font-size: 11pt; font-family: verdana">',  '<strong>Description:</strong> ', IIF(pmd.description = '', 'No description in top of module', REPLACE(pmd.description, '/*', '')),CHAR(13), '</p>') AS description,
       CONCAT('<h4  style="font-family: verdana;color:#2f5596; font-weight: normal; font-size:11pt">','Definition:','</h4>' , CHAR(13)) AS spacer,
       CONCAT( '<hr/ style="border-top: dotted 2px;"><p>','<div style="font-size: 10pt; font-family: consolas,monaco,monospace;color:#5B5959; padding: 20px;margin:10px; background-color: #F5F3F3 ">',
	   IIF(cmt_start > 0, CONCAT(SUBSTRING(pmd.definition, 0, cmt_start), SUBSTRING(pmd.definition, cmt_end +2, LEN(pmd.definition) - cmt_end)) , pmd.definition ),
	    '</div>','<hr/  style="border-top: dotted 2px;"></p>') AS definition
       --REPLACE(pmd.definition, CONCAT( '/* ', pmd.description, ' */') , CHAR(13)) AS definition
INTO dbo.documentation
FROM prepared_modules_data pmd



IF (OBJECT_ID('tempdb..#result', 'U') IS NOT NULL)
	DROP TABLE #result

IF (OBJECT_ID('tempdb..#temp_module', 'U') IS NOT NULL)
	DROP TABLE #temp_module

IF (OBJECT_ID('tempdb..#lookup', 'U') IS NOT NULL)
	DROP TABLE #lookup

IF (SELECT COUNT(*)
    FROM dbo.documentation) > 0
BEGIN

	EXEC master.dbo.sp_configure 'show advanced options'
	,                            1
	RECONFIGURE
	EXEC master.dbo.sp_configure 'xp_cmdshell'
	,                            1
	RECONFIGURE

	RAISERROR ('--| Building txt file', 0, 1) WITH NOWAIT

	DECLARE @sql_cmd_text NVARCHAR(4000) = CONCAT('bcp "SELECT * FROM ',QUOTENAME(DB_NAME()),'.dbo.documentation ORDER BY 1" queryout "C:\temp\documentation.html" -T -w -t \n -C 1252')
	EXEC xp_cmdshell @sql_cmd_text

	EXEC master.dbo.sp_configure 'xp_cmdshell'
	,                            0
	RECONFIGURE

END
ELSE 
BEGIN
RAISERROR ('--| No modules retrieved', 0, 1) WITH NOWAIT
END


IF (OBJECT_ID('dbo.documentation', 'U') IS NOT NULL)
	DROP TABLE dbo.documentation


RAISERROR ('-| Done', 0, 1) WITH NOWAIT
