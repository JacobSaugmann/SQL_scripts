WITH IndexStats AS (
    SELECT
        s.name AS SchemaName,
        t.name AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent,
        ips.page_count,
        ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) AS TotalReads
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ips
    INNER JOIN sys.indexes AS i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.dm_db_index_usage_stats AS us ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = DB_ID()
    WHERE ips.avg_fragmentation_in_percent > 30
      AND ips.page_count > 500  -- Kun store indeks
)
SELECT
    SchemaName,
    TableName,
    ISNULL(IndexName, '(ALL)') AS IndexName,
    avg_fragmentation_in_percent AS FragmentationPct,
    page_count AS PageCount,
    TotalReads,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 'REBUILD'
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 'REORGANIZE'
        ELSE 'IGNORE'
    END AS ActionRecommendation,
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN
            CASE WHEN IndexName IS NULL
                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REBUILD WITH (ONLINE = OFF);'
            END
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN
            CASE WHEN IndexName IS NULL
                THEN 'ALTER INDEX ALL ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
                ELSE 'ALTER INDEX [' + IndexName + '] ON [' + SchemaName + '].[' + TableName + '] REORGANIZE;'
            END
        ELSE '-- IGNORE: Low usage or low fragmentation'
    END AS SQLCommand
FROM IndexStats
ORDER BY
    CASE
        WHEN avg_fragmentation_in_percent > 80 AND TotalReads > 1000 THEN 1
        WHEN avg_fragmentation_in_percent BETWEEN 30 AND 80 AND TotalReads > 100 THEN 2
        ELSE 3
    END,
    avg_fragmentation_in_percent DESC,
    TotalReads DESC;

