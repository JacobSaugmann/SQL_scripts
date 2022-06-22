/* Query the statistics witch is in use*/

DECLARE @tableName sysname = 'person';

SELECT so.name,
       st.name,
       st.stats_id,
       sc.stats_column_id,
       c.name AS column_name,
       st.auto_created,
       st.filter_definition,
       sp.last_updated,
       sp.rows,
       sp.rows_sampled,
       sp.stats_id,
       sp.modification_counter
FROM sys.stats st
    INNER JOIN sys.stats_columns sc
        ON st.object_id = sc.object_id
           AND st.stats_id = sc.stats_id
    INNER JOIN sys.columns c
        ON sc.object_id = c.object_id
           AND sc.column_id = c.column_id
    INNER JOIN sys.objects so
        ON st.object_id = so.object_id
    CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
WHERE so.name = @tableName
ORDER BY so.name,
         st.stats_id,
         sc.stats_column_id;