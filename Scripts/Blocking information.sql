--SELECT o.name
--FROM sys.partitions p
--INNER JOIN sys.objects o ON p.object_id = o.object_id
--WHERE p.hobt_id = 72057598417108992

DROP TABLE IF EXISTS #Blocking

SELECT TL.resource_type,
	   TL.resource_database_id,
	   TL.resource_associated_entity_id,
	   TL.request_mode,
	   TL.request_session_id,
	   WT.blocking_session_id,
	   O.name AS [object name],
	   O.type_desc AS [object descr],
	   P.partition_id AS [partition id],
	   P.rows AS [partition/page rows],
	   AU.type_desc AS [index descr],
	   AU.container_id AS [index/page container_id]
	INTO #Blocking
FROM sys.dm_tran_locks AS TL                  
	INNER JOIN sys.dm_os_waiting_tasks AS WT  
			ON TL.lock_owner_address = WT.resource_address
	LEFT OUTER JOIN sys.objects AS O          
			ON O.object_id = TL.resource_associated_entity_id
	LEFT OUTER JOIN sys.partitions AS P       
			ON P.hobt_id = TL.resource_associated_entity_id
	LEFT OUTER JOIN sys.allocation_units AS AU
			ON AU.allocation_unit_id = TL.resource_associated_entity_id
WHERE WT.blocking_session_id IS NOT NULL;;

SELECT *
FROM #Blocking

SELECT s.session_id,
	   s.host_name,
	   s.login_name,
	   s.arithabort,
	   s.context_info,
	   r.last_wait_type,
	   r.open_transaction_count,
	   qp.query_plan
FROM sys.dm_exec_sessions s                             
	LEFT JOIN sys.dm_exec_requests r                    
			ON s.session_id = r.session_id
	CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) qp
WHERE s.session_id IN (SELECT DISTINCT blocking_session_id
                       FROM #Blocking)


EXEC sp_WhoIsActive
