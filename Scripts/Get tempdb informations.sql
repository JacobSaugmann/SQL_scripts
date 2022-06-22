USE tempdb
GO

--Currently running transactions
SELECT t.transaction_id,
	   t.name,
	   t.transaction_type,
	   CASE t.transaction_state 
			WHEN 0 THEN 'not initialized yet.'
			WHEN 1 THEN 'initialized - not started.'
			WHEN 2 THEN 'transaction is active.'
			WHEN 3 THEN 'The transaction has ended. (read-only).'
			WHEN 4 THEN 'The commit process has been initiated.' 
			WHEN 5 THEN 'Prepared state and waiting resolution.'
			WHEN 6 THEN 'The transaction has been committed.'
			WHEN 7 THEN 'The transaction is being rolled back.'
			WHEN 8 THEN 'The transaction has been rolled back.'
			END AS transaction_state,
	   s.transaction_id,
	   s.session_id,
	   s.elapsed_time_seconds/60/60.0 AS hours_tran_has_been_open,
	   p.status,
	   p.cmd
FROM sys.dm_tran_active_transactions t                      
	JOIN sys.dm_tran_active_snapshot_database_transactions s
			ON t.transaction_id = s.transaction_id
	JOIN sys.sysprocesses p                                 
			ON p.spid = s.session_id
GO

--Spaced used by version store
SELECT SUM (version_store_reserved_page_count)*8.0 /1024/1024 AS version_store_gb,
	   SUM (unallocated_extent_page_count)*8.0 /1024/1024 AS freespace_gb,
	   SUM (mixed_extent_page_count)*8.0 /1024/1024 AS mixedextent_gb
FROM sys.dm_db_file_space_usage

--130.8 GB


--Get running queries that takes a lot of TEMPDB space
SELECT es.host_name,
	   es.login_name,
	   es.program_name,
	   st.dbid AS QueryExecContextDBID,
	   DB_NAME(st.dbid) AS QueryExecContextDBNAME,
	   st.objectid AS ModuleObjectId,
	   SUBSTRING(st.text, er.statement_start_offset/2 + 1,(CASE WHEN er.statement_end_offset = -1 THEN LEN(CONVERT(nvarchar(MAX),st.text)) * 2
	                                                                                              ELSE er.statement_end_offset END - er.statement_start_offset)/2) AS Query_Text,
	   tsu.session_id,
	   tsu.request_id,
	   tsu.exec_context_id,
	   (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) AS OutStanding_user_objects_page_counts,
	   (tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count) AS OutStanding_internal_objects_page_counts,
	   er.start_time,
	   er.command,
	   er.open_transaction_count,
	   er.percent_complete,
	   er.estimated_completion_time,
	   er.cpu_time,
	   er.total_elapsed_time,
	   er.reads,
	   er.writes,
	   er.logical_reads,
	   er.granted_query_memory
FROM sys.dm_db_task_space_usage tsu                   
	INNER JOIN sys.dm_exec_requests er                
			ON (
					tsu.session_id = er.session_id
					AND tsu.request_id = er.request_id)
	INNER JOIN sys.dm_exec_sessions es                
			ON ( tsu.session_id = es.session_id )
	CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) st
WHERE (tsu.internal_objects_alloc_page_count+tsu.user_objects_alloc_page_count) > 0
ORDER BY (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count)+(tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count)
DESC
GO


--Get running transactions on tempdb with additional information
SELECT TOP 5 a.session_id,
	         a.transaction_id,
	         a.transaction_sequence_num,
	         a.elapsed_time_seconds,
	         b.program_name,
	         b.open_tran,
	         b.status,
	         DB_NAME(x.database_id) AS database_name,
	         x.blocking_session_id,
	         x.cpu_time,
	         x.total_elapsed_time,
	         x.logical_reads,
	         x.dop,
	         p.query_plan
FROM sys.dm_tran_active_snapshot_database_transactions a  
	JOIN sys.sysprocesses b                               
			ON a.session_id = b.spid
	CROSS APPLY (SELECT *
                 FROM sys.dm_exec_requests r
                 WHERE r.session_id = a.session_id) x     
	CROSS APPLY sys.dm_exec_query_plan(x.plan_handle) AS p
ORDER BY elapsed_time_seconds DESC