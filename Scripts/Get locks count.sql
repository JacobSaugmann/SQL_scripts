/* Query to get locked objects and lock count */

DROP TABLE IF EXISTS #locks

SELECT DB_NAME(l.resource_database_id) AS database_name,
	   CASE WHEN resource_type = 'object' THEN OBJECT_NAME(l.resource_associated_entity_id)
	                                      ELSE OBJECT_NAME(p.OBJECT_ID) END AS object_name,
	   l.resource_type,
	   l.request_type,
	   l.request_status,
	   l.request_session_id AS spid
	INTO #locks
FROM sys.dm_tran_locks l         
	LEFT JOIN sys.partitions AS p
			ON p.hobt_id = l.resource_associated_entity_id
--ORDER BY database_name, object_name, spid


SELECT COUNT(*) locks,
	   database_name,
	   object_name,
	   request_status,
	   resource_type,
	   spid
FROM #locks
GROUP BY database_name , object_name , request_status , resource_type , spid
ORDER BY locks DESC

SELECT DISTINCT COUNT(*) OVER (PARTITION BY database_name,object_name,request_status,resource_type,spid) AS locks,
	            l.*,
	            (COALESCE(s.total_elapsed_time,0) / (1000 * 60)) % 60 AS total_elapsed_time_in_minutes,
	            s.host_name,
	            s.program_name
FROM #locks l                       
	LEFT JOIN sys.dm_exec_sessions s
			ON l.spid = s.session_id
WHERE object_name IS NOT NULL
ORDER BY locks DESC , database_name , object_name

DROP TABLE IF EXISTS #locks