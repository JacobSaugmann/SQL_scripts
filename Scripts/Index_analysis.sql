SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON
--USE [DatabaseName]
--GO

/*
==========================================================================
INDEX ANALYSIS SCRIPT v2
==========================================================================
Kompatibilitet: SQL Server 2012 (v11) til 2025 (v17).

PARAMETRE
---------
@db_id                   : Database der analyseres (default: aktuel DB)
@min_advantage           : Minimum impact-procent for missing index forslag
@getselectability        : 1 = beregn density/selectability per kolonne
@drop_tmp_table          : 1 = ryd op i temp-tabeller efter kørsel
@only_index_analysis     : 1 = spring overlap/drop/wasted-space over
@limit_to_tablename      : LIKE-filter på tabel-navn (tom streng = alle)
@limit_to_indexname      : LIKE-filter på index-navn (tom streng = alle)

USAGE-RATIOS (tilfojet i denne version)
----------------------------------------
read_write_ratio = (seeks + scans + lookups) / updates
  >10   : Staerk vaerdi - reads dominerer kraftigt
  1-10  : Vaerdifuldt - flere reads end writes
  <1    : Drop-kandidat - flere writes end reads
  NULL  : updates=0 - rent laest index (best case)

seek_ratio = seeks / (seeks + scans + lookups)
  >0.9    : Optimizer bruger indexet via seeks - designet rigtigt
  0.5-0.9 : Blandet - ofte ok, men tjek om range scans er forventet
  <0.5    : Mest scans/lookups - leading key matcher sjaeldent praedikater
  NULL    : Aldrig brugt

reads_per_million_rows = total_reads * 1.000.000 / records
  >100  : Hot index - meget brugt relativt til tabel-stoerrelse
  1-100 : Moderat brug
  <1    : Sjaelden brug

KATEGORISERING (Messages-fanen)
-------------------------------
High impact     : total_reads >= 100.000 AND read_write_ratio >= 10
Good impact     : total_reads >= 1.000  AND read_write_ratio >= 1
Low impact      : Reads dominerer writes, men lavt total brug
Drop candidates : read_write_ratio < 1 ELLER total_reads = 0

DENSITY OG SELECTABILITY
------------------------
column_density     = 1 / distinct_count
                     (matcher SQL Server-statistikkens 'all_density')
                     Lav vaerdi er god - mange distinkte vaerdier
column_selectivity = distinct_count / row_count
                     Hoej vaerdi er god - taet paa 1 betyder unik kolonne

OPTIMERING: Bruger APPROX_COUNT_DISTINCT() paa SQL 2019+ (5-20x hurtigere
end COUNT DISTINCT). Falder tilbage til COUNT DISTINCT paa aeldre versioner.

VIGTIGT: Tjek meta_data_age foer beslutninger. DMV-tal nulstilles ved
server-restart og er upaalidelige de foerste 14 dage.
==========================================================================
*/

-- =====================================================================
-- Version-check
-- =====================================================================
DECLARE @ProductVersion NVARCHAR(128) = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))
DECLARE @ProductMajorVersion INT =
    CAST(LEFT(@ProductVersion, CHARINDEX('.', @ProductVersion) - 1) AS INT)

IF @ProductMajorVersion < 11
BEGIN
    DECLARE @verr NVARCHAR(200) =
        N'Scriptet kraever SQL Server 2012 (version 11) eller nyere. Fundet: '
        + @ProductVersion
    RAISERROR(@verr, 16, 1)
    RETURN
END

-- APPROX_COUNT_DISTINCT kraever SQL 2019 (major version 15) eller nyere
DECLARE @UseApproxCount BIT = CASE WHEN @ProductMajorVersion >= 15 THEN 1 ELSE 0 END

-- =====================================================================
-- Konfiguration
-- =====================================================================
DECLARE @db_id SMALLINT = DB_ID()
DECLARE @min_advantage TINYINT = 80
DECLARE @getselectability BIT = 0
DECLARE @drop_tmp_table BIT = 0
DECLARE @meta_age INT = -1
DECLARE @only_index_analysis BIT = 1
DECLARE @limit_to_tablename VARCHAR(512) = ''
DECLARE @limit_to_indexname VARCHAR(512) = ''

-- Defensiv: undgaa NULL-faelder paa LIKE-filtre
SET @limit_to_tablename = COALESCE(@limit_to_tablename, '')
SET @limit_to_indexname = COALESCE(@limit_to_indexname, '')

-- =====================================================================
-- Auto-fix af konflikterende parametre
-- =====================================================================
-- Hvis brugeren beder om selectability/overlap/drop/wasted-space, skal
-- @only_index_analysis vaere 0 - ellers springer GOTO MESSAGES_OUTPUT
-- over de relevante blokke. Override stille og sig det til brugeren.
IF @only_index_analysis = 1 AND @getselectability = 1
BEGIN
    RAISERROR('NOTE: @getselectability = 1 kraever @only_index_analysis = 0 - overrider automatisk til 0.', 0, 1) WITH NOWAIT
    SET @only_index_analysis = 0
END

-- Advarsel hvis selectability bedes uden tabel-filter (meget dyrt paa store DB)
IF @getselectability = 1 AND @limit_to_tablename = ''
BEGIN
    RAISERROR('WARNING: @getselectability = 1 uden @limit_to_tablename kan tage TIMER paa store databaser. Overvej at saette @limit_to_tablename foer du fortsaetter.', 0, 1) WITH NOWAIT
END

-- =====================================================================
-- Oprydning af temp-tabeller fra evt. tidligere koersler
-- =====================================================================
IF OBJECT_ID('tempdb..#MissingIndexes','U') IS NOT NULL DROP TABLE #MissingIndexes
IF OBJECT_ID('tempdb..#ExistingIndexes','U') IS NOT NULL DROP TABLE #ExistingIndexes
IF OBJECT_ID('tempdb..#useless_space_consumption','U') IS NOT NULL DROP TABLE #useless_space_consumption

-- =====================================================================
-- Metadata-alder (tempdb-uptime som proxy for server-uptime)
-- =====================================================================
SELECT @meta_age = CASE WHEN DATEDIFF(DAY, d.create_date, GETDATE()) = 0 THEN 1
                        ELSE DATEDIFF(DAY, d.create_date, GETDATE())
                   END
FROM sys.databases d
WHERE d.name = 'tempdb'

IF @meta_age < 14
BEGIN
    DECLARE @msg NVARCHAR(300) = N'Warning! metadata is only ' + CAST(@meta_age AS NVARCHAR(3))
        + N' days old, the data should be at least 14 days old for a more precise result'
    RAISERROR(@msg, 0, 1) WITH NOWAIT
END

-- =====================================================================
-- MISSING INDEXES
-- =====================================================================
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
       @meta_age AS meta_data_age,
       CONCAT('CREATE NONCLUSTERED INDEX idx_',
              REPLACE(REPLACE(REPLACE(
                  SUBSTRING(md.statement,
                            CHARINDEX('.', md.statement, CHARINDEX('.', md.statement)+1)+1,
                            LEN(md.statement) - LEN(CHARINDEX('.', md.statement, CHARINDEX('.', md.statement)+1))),
                  '[',''), ']',''),'.','_'),
              '_', FORMAT(GETDATE(), 'yyyyMMdd'),
              ' ON ', md.statement,
              ' (', IIF(md.equality_columns IS NOT NULL, md.equality_columns, md.inequality_columns), ')',
              IIF(md.included_columns IS NOT NULL,
                  CONCAT(CHAR(13), 'INCLUDE(' + md.included_columns + ')'),
                  ''),
              CHAR(13), 'WITH (DATA_COMPRESSION=PAGE);') AS create_ix_stmt
INTO #MissingIndexes
FROM sys.dm_db_missing_index_details md
    INNER JOIN sys.dm_db_missing_index_groups mg
            ON mg.index_handle = md.index_handle
    INNER JOIN sys.dm_db_missing_index_group_stats mgs
            ON mg.index_group_handle = mgs.group_handle
WHERE md.database_id = @db_id
    AND (mgs.avg_user_impact > @min_advantage
         OR mgs.avg_total_user_cost > 50)
    AND md.statement LIKE '%' + @limit_to_tablename + '%'
ORDER BY md.object_id, mgs.avg_user_impact DESC

-- =====================================================================
-- EXISTING INDEXES (LEFT JOIN paa usage_stats - inkluderer ubrugte!)
-- Med tilfojede usage-ratios.
-- =====================================================================
SELECT CONCAT(QUOTENAME(DB_NAME(@db_id)),'.', QUOTENAME(s.name),'.',QUOTENAME(t.name)) AS table_name,
       'Existing_Index' AS table_type_desc,
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
       COALESCE(ius.user_lookups, 0) AS user_lookups,
       COALESCE(ius.user_scans, 0)   AS user_scans,
       COALESCE(ius.user_seeks, 0)   AS user_seeks,
       COALESCE(ius.user_updates, 0) AS user_updates,
       ius.last_user_update,
       -- read_write_ratio: FLOAT-aritmetik for at undgaa DECIMAL precision/scale overflow.
       -- Cap til 10^15 for at sikre CAST AS DECIMAL(38,2) altid lykkes.
       CASE
           WHEN COALESCE(ius.user_updates, 0) = 0 THEN NULL
           WHEN CAST(COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0) AS FLOAT)
                / ius.user_updates > 1e15
               THEN CAST(1000000000000000 AS DECIMAL(38,2))
           ELSE CAST(
               CAST(COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0) AS FLOAT)
               / ius.user_updates
               AS DECIMAL(38,2)
           )
       END AS read_write_ratio,
       -- seek_ratio: altid mellem 0 og 1, FLOAT giver tilstraekkelig praecision.
       CASE
           WHEN (COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0)) = 0
                THEN NULL
           ELSE CAST(
               CAST(COALESCE(ius.user_seeks, 0) AS FLOAT)
               / (COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0))
               AS DECIMAL(18,4)
           )
       END AS seek_ratio,
       -- reads_per_million_rows: FLOAT-aritmetik, divider foer multiplikation.
       -- Cap til 10^15 for at sikre CAST AS DECIMAL(38,2) altid lykkes.
       CASE
           WHEN COALESCE(ids.record_count, 0) = 0 THEN NULL
           WHEN CAST(COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0) AS FLOAT)
                / ids.record_count * 1000000.0 > 1e15
               THEN CAST(1000000000000000 AS DECIMAL(38,2))
           ELSE CAST(
               CAST(COALESCE(ius.user_seeks,0) + COALESCE(ius.user_scans,0) + COALESCE(ius.user_lookups,0) AS FLOAT)
               / ids.record_count * 1000000.0
               AS DECIMAL(38,2)
           )
       END AS reads_per_million_rows,
       @meta_age AS meta_data_age
INTO #ExistingIndexes
FROM sys.indexes i
    CROSS APPLY sys.dm_db_index_physical_stats(@db_id, i.object_id, i.index_id, NULL, 'SAMPLED') ids
    LEFT JOIN sys.dm_db_index_usage_stats ius
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
            FROM sys.index_columns INXCLS
                INNER JOIN sys.columns CLS
                        ON INXCLS.object_id = CLS.object_id
                            AND INXCLS.column_id = CLS.column_id
            WHERE i.object_id = INXCLS.object_id
                AND i.index_id = INXCLS.index_id
                AND INXCLS.is_included_column = 0
                AND i.name LIKE '%' + @limit_to_indexname + '%'
            FOR XML PATH('')
            ), 1, 1, '')
        ) DS1([index_columns_names])
    CROSS APPLY (
        SELECT STUFF((
            SELECT ' [' + CLS.[name] + '];'
            FROM sys.index_columns INXCLS
                INNER JOIN sys.columns CLS
                        ON INXCLS.object_id = CLS.object_id
                            AND INXCLS.column_id = CLS.column_id
            WHERE i.object_id = INXCLS.object_id
                AND i.index_id = INXCLS.index_id
                AND INXCLS.is_included_column = 1
                AND i.name LIKE '%' + @limit_to_indexname + '%'
            FOR XML PATH('')
            ), 1, 1, '')
        ) DS2([included_columns_names])
WHERE i.type IN (1,2,5)
    AND ids.database_id = @db_id
    AND alloc_unit_type_desc = 'IN_ROW_DATA'
    AND i.is_disabled = 0
    AND t.name LIKE '%' + @limit_to_tablename + '%'

-- =====================================================================
-- KOMBINERET OUTPUT: Missing + Existing pivoteret pr tabel
-- =====================================================================
;WITH INDEX_INFO AS
(
    SELECT m.table_name COLLATE DATABASE_DEFAULT AS table_name,
           m.table_type_desc COLLATE DATABASE_DEFAULT AS table_type_desc,
           NULL AS index_name,
           NULL AS index_type_desc,
           NULL AS is_unique,
           NULL AS is_primary_key,
           NULL AS has_filter,
           m.equality_columns COLLATE DATABASE_DEFAULT AS index_columns_names,
           m.included_columns COLLATE DATABASE_DEFAULT AS included_columns,
           m.inequality_columns COLLATE DATABASE_DEFAULT AS inequality_columns,
           m.user_scans,
           m.user_seeks,
           NULL AS user_updates,
           CAST(NULL AS DECIMAL(38,2)) AS read_write_ratio,
           CAST(NULL AS DECIMAL(18,4)) AS seek_ratio,
           CAST(NULL AS DECIMAL(38,2)) AS reads_per_million_rows,
           CAST(m.avg_user_impact AS DECIMAL(18,2)) AS avg_user_impact,
           CAST(m.avg_total_user_cost AS DECIMAL(18,2)) AS avg_total_user_cost,
           CAST((CAST(m.avg_total_user_cost AS DECIMAL(38,4)) * (m.avg_user_impact /100.0) * m.user_seeks) / NULLIF(meta_data_age,0)
                AS DECIMAL(38,2)) AS create_index_adv,
           m.meta_data_age,
           m.create_ix_stmt AS create_index_statement
    FROM #MissingIndexes m
    UNION ALL
    SELECT DISTINCT
           e.table_name,
           e.table_type_desc,
           e.name,
           e.type_desc,
           e.is_unique,
           e.is_primary_key,
           e.has_filter,
           e.index_columns_names,
           e.included_columns_names,
           NULL AS inequality_columns,
           e.user_scans,
           e.user_seeks,
           e.user_updates,
           e.read_write_ratio,
           e.seek_ratio,
           e.reads_per_million_rows,
           NULL AS avg_user_impact,
           NULL AS avg_total_user_cost,
           NULL AS create_index_adv,
           e.meta_data_age,
           NULL AS create_index_statement
    FROM #ExistingIndexes e
)
SELECT *
FROM INDEX_INFO
ORDER BY table_name, table_type_desc COLLATE DATABASE_DEFAULT

IF @only_index_analysis = 1
    GOTO MESSAGES_OUTPUT

-- =====================================================================
-- OVERLAPPING INDEXES
-- Detekterer baade identiske og leading-key overlap.
-- =====================================================================
;WITH OVERLAPPING_INDEXES AS
(
    -- Identisk match (samme key-kolonner)
    SELECT e.table_name,
           e.name AS index_name,
           e.index_columns_names,
           e.included_columns_names,
           CASE WHEN COALESCE(es.included_columns_names, '') = COALESCE(e.included_columns_names, '')
                THEN 'Overlapping (identical keys)'
                ELSE 'Nearby overlapping (identical keys, different INCLUDE)'
           END AS index_state,
           ROW_NUMBER() OVER (PARTITION BY e.index_columns_names, e.table_name ORDER BY e.name) AS row_no
    FROM #ExistingIndexes e
        INNER JOIN #ExistingIndexes es
                ON es.index_columns_names = e.index_columns_names
                    AND es.table_name = e.table_name
                    AND es.name <> e.name
    UNION ALL
    -- Leading-key prefix match: e er prefix af e2
    SELECT e.table_name,
           e.name AS index_name,
           e.index_columns_names,
           e.included_columns_names,
           'Leading-key subset (dekkes af bredere index)' AS index_state,
           2 AS row_no  -- altid drop-kandidat naar et bredere index findes
    FROM #ExistingIndexes e
        INNER JOIN #ExistingIndexes e2
                ON e2.table_name = e.table_name
                    AND e2.name <> e.name
                    AND LEN(e2.index_columns_names) > LEN(e.index_columns_names)
                    AND LEFT(e2.index_columns_names, LEN(e.index_columns_names)) = e.index_columns_names
)
SELECT DISTINCT
       CASE WHEN oi.row_no > 1 AND oi.index_state LIKE 'Overlapping%'
                THEN '[WARNING] overlapping index'
            WHEN oi.row_no > 1 AND oi.index_state LIKE 'Leading-key%'
                THEN '[WARNING] leading-key subset'
            WHEN oi.row_no > 1 AND oi.index_state LIKE 'Nearby%'
                THEN '[INFO] nearby overlapping index'
            ELSE NULL
       END AS msg,
       oi.table_name,
       oi.index_name,
       oi.index_columns_names,
       oi.included_columns_names,
       oi.index_state,
       CASE WHEN oi.row_no > 1
            THEN CONCAT('ALTER INDEX ', QUOTENAME(oi.index_name), ' ON ', oi.table_name, ' DISABLE;')
       END AS disable_stmt
FROM OVERLAPPING_INDEXES oi
WHERE oi.table_name LIKE '%' + @limit_to_tablename + '%'
    OR oi.index_name LIKE '%' + @limit_to_indexname + '%'
ORDER BY oi.table_name, oi.index_name

IF @meta_age < 100
BEGIN
    SET @msg = N'Warning! metadata is only ' + CAST(@meta_age AS NVARCHAR(3))
        + N' days old, remember some indexes may only be used every 3, 6 months but can have significant performance impact'
    RAISERROR(@msg, 0, 1) WITH NOWAIT
END

-- =====================================================================
-- DROP CANDIDATES
-- Indexes hvor 95%+ af aktivitet er updates (Jacobs originale formel).
-- =====================================================================
SELECT e.table_name,
       CONCAT('DROP INDEX ', QUOTENAME(e.name), ' ON ', e.table_name) AS drop_statement,
       CONCAT('ALTER INDEX ', QUOTENAME(e.name), ' ON ', e.table_name, ' DISABLE;') AS disable_statement,
       e.user_lookups,
       e.user_scans,
       e.user_seeks,
       e.user_updates,
       e.read_write_ratio,
       e.meta_data_age
FROM #ExistingIndexes e
WHERE (((IIF(e.user_updates=0, 1.0, e.user_updates*1.0)
         - (e.user_scans + e.user_seeks + e.user_lookups))
        / IIF(e.user_updates=0, 1, e.user_updates)) * 100 > 95)
    AND e.is_primary_key = 0
    AND e.is_unique = 0
    AND e.table_name LIKE '%' + @limit_to_tablename + '%'
ORDER BY e.table_name

-- =====================================================================
-- DENSITY OG SELECTABILITY (kun naar @getselectability = 1)
-- Bruger APPROX_COUNT_DISTINCT paa SQL 2019+ for hastighed.
-- =====================================================================
IF @getselectability = 1
BEGIN
    RAISERROR('Now calculating density/selectability, please be patient!', 0, 1) WITH NOWAIT

    IF @UseApproxCount = 1
        RAISERROR('Using APPROX_COUNT_DISTINCT (SQL 2019+) - ~2 pct margin, much faster.', 0, 1) WITH NOWAIT
    ELSE
        RAISERROR('Using COUNT(DISTINCT) - exact but slow on large tables.', 0, 1) WITH NOWAIT

    IF OBJECT_ID('tempdb..#Density_Selectability','U') IS NOT NULL
        DROP TABLE #Density_Selectability

    CREATE TABLE #Density_Selectability (
        table_name           SYSNAME,
        column_name          SYSNAME,
        row_count            BIGINT DEFAULT(1),
        distinct_count       BIGINT NULL,
        column_density       DECIMAL(18,12),     -- 1 / distinct (low = good)
        column_selectivity   DECIMAL(18,12),     -- distinct / rows (high = good)
        occurrences          INT,
        max_length_byte      INT NULL
    )

    DECLARE @tableName SYSNAME
    DECLARE @columnName SYSNAME
    DECLARE @rowcount BIGINT
    DECLARE @occurrences INT

    DECLARE density_cursor CURSOR LOCAL FAST_FORWARD FOR
    WITH ROW_COUNT_CTE AS
    (
        SELECT CONCAT(QUOTENAME(DB_NAME(DB_ID())),'.',QUOTENAME(SCHEMA_NAME(schema_id)),'.',QUOTENAME(t.name)) AS TableName,
               SUM(p.rows) AS TotalRowCount
        FROM sys.tables AS t
            INNER JOIN sys.partitions AS p
                    ON t.object_id = p.object_id
                        AND p.index_id IN (0, 1)
                        AND t.name LIKE '%' + @limit_to_tablename + '%'
        GROUP BY SCHEMA_NAME(schema_id), t.name
    )
    SELECT DISTINCT
           mi.table_name,
           mi.index_column_name,
           CASE WHEN TotalRowCount = 0 THEN 1 ELSE TotalRowCount END AS TotalRowCount,
           mi.occurrences
    FROM (
        SELECT tmp.table_name,
               tmp.index_column_name,
               tmp.TotalRowCount,
               COUNT(*) OVER (PARTITION BY tmp.table_name, tmp.index_column_name) AS occurrences
        FROM (
            SELECT table_name,
                   LTRIM(RTRIM(m.n.value('.[1]','nvarchar(128)'))) AS index_column_name,
                   r.TotalRowCount
            FROM (
                SELECT table_name,
                       CAST('<XMLRoot><RowData>' + REPLACE(e.equality_columns,',','</RowData><RowData>')
                            + '</RowData></XMLRoot>' AS XML) AS x
                FROM #MissingIndexes e
                WHERE e.equality_columns IS NOT NULL
            ) t
            LEFT JOIN ROW_COUNT_CTE r
                    ON t.table_name = r.TableName COLLATE DATABASE_DEFAULT
            CROSS APPLY x.nodes('/XMLRoot/RowData') m(n)
        ) tmp
        WHERE index_column_name <> N''
    ) mi
    OPTION(RECOMPILE)

    OPEN density_cursor
    FETCH NEXT FROM density_cursor INTO @tableName, @columnName, @rowcount, @occurrences

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Vaelg distinct-funktion baseret paa SQL Server-version.
        -- APPROX_COUNT_DISTINCT er ~2 pct margin men 5-20x hurtigere.
        DECLARE @distinct_expr NVARCHAR(200) =
            CASE WHEN @UseApproxCount = 1
                 THEN N'APPROX_COUNT_DISTINCT(' + @columnName + N')'
                 ELSE N'COUNT(DISTINCT ' + @columnName + N')'
            END

        DECLARE @statement NVARCHAR(MAX) = N'
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            WITH Stats AS (
                SELECT ' + @distinct_expr + N' AS distinct_count
                FROM ' + @tableName + N'
            )
            SELECT
                @p_table   AS table_name,
                @p_column  AS column_name,
                @p_rows    AS row_count,
                s.distinct_count,
                CASE WHEN s.distinct_count = 0 THEN NULL
                     ELSE 1.0 / s.distinct_count
                END AS column_density,
                CASE WHEN @p_rows = 0 THEN NULL
                     ELSE CAST(s.distinct_count AS DECIMAL(18,2)) / @p_rows
                END AS column_selectivity,
                @p_occurrences AS occurrences,
                c.max_length
            FROM Stats s
                INNER JOIN (
                    SELECT c.max_length, QUOTENAME(c.name) AS column_name
                    FROM sys.columns c
                    WHERE c.object_id = OBJECT_ID(@p_table, ''U'')
                ) c ON c.column_name = @p_column'

        BEGIN TRY
            INSERT INTO #Density_Selectability (
                table_name, column_name, row_count, distinct_count,
                column_density, column_selectivity, occurrences, max_length_byte
            )
            EXEC sp_executesql @statement,
                N'@p_table SYSNAME, @p_column SYSNAME, @p_rows BIGINT, @p_occurrences INT',
                @p_table = @tableName,
                @p_column = @columnName,
                @p_rows = @rowcount,
                @p_occurrences = @occurrences
        END TRY
        BEGIN CATCH
            DECLARE @ErrorMsg NVARCHAR(4000) =
                CONCAT('Error on ', @tableName, '.', @columnName, ': ', ERROR_MESSAGE())
            RAISERROR(@ErrorMsg, 0, 1) WITH NOWAIT
        END CATCH

        FETCH NEXT FROM density_cursor INTO @tableName, @columnName, @rowcount, @occurrences
    END

    CLOSE density_cursor
    DEALLOCATE density_cursor

    SELECT *
    FROM #Density_Selectability
    ORDER BY table_name, column_selectivity DESC
END

-- =====================================================================
-- WASTED SPACE BY UNUSED INDEXES
-- Fix: brug 1024.0 (decimal) for at undgaa integer division paa indexes < 1 MB.
-- =====================================================================
;WITH DROP_INDEXES AS
(
    SELECT e.name,
           e.record_count
    FROM #ExistingIndexes e
    WHERE (((IIF(e.user_updates=0, 1.0, e.user_updates*1.0)
             - (e.user_scans + e.user_seeks + e.user_lookups))
            / IIF(e.user_updates=0, 1, e.user_updates)) * 100 > 95)
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
    INNER JOIN DROP_INDEXES di
            ON di.name = i.name COLLATE DATABASE_DEFAULT
    OUTER APPLY (
        SELECT COUNT(sz.used_page_count) AS page_count,
               CAST(SUM(sz.used_page_count) * 8.0 / 1024 AS DECIMAL(18,2)) AS mb_pages
        FROM sys.dm_db_partition_stats AS sz
            INNER JOIN sys.indexes AS ix
                    ON sz.object_id = ix.object_id
                        AND sz.index_id = ix.index_id
            INNER JOIN sys.tables tn
                    ON tn.object_id = ix.object_id
        WHERE i.name = ix.name
            AND i.object_id = ix.object_id
            AND tn.name LIKE '%' + @limit_to_tablename + '%'
        GROUP BY tn.name, ix.name
    ) AS size

IF (SELECT COALESCE(SUM(u.mb_pages), 0) FROM #useless_space_consumption u) > 0
BEGIN
    SELECT 'At least ' + CAST(COALESCE(SUM(u.mb_pages), 0) AS VARCHAR(50)) + ' MB of wasted space' AS comment
    FROM #useless_space_consumption u
END

-- =====================================================================
-- KATEGORISERING TIL MESSAGES-FANEN
-- =====================================================================
MESSAGES_OUTPUT:

DECLARE @categorized TABLE (
    category   VARCHAR(20),
    sort_order INT,
    line       NVARCHAR(MAX)
)

INSERT INTO @categorized
SELECT
    CASE
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) = 0
             THEN 'Drop candidates'
        WHEN e.read_write_ratio IS NOT NULL AND e.read_write_ratio < 1
             THEN 'Drop candidates'
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) >= 100000
             AND (e.read_write_ratio IS NULL OR e.read_write_ratio >= 10)
             THEN 'High impact'
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) >= 1000
             AND (e.read_write_ratio IS NULL OR e.read_write_ratio >= 1)
             THEN 'Good impact'
        ELSE 'Low impact'
    END,
    CASE
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) = 0 THEN 4
        WHEN e.read_write_ratio IS NOT NULL AND e.read_write_ratio < 1 THEN 4
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) >= 100000
             AND (e.read_write_ratio IS NULL OR e.read_write_ratio >= 10) THEN 1
        WHEN (e.user_seeks + e.user_scans + e.user_lookups) >= 1000
             AND (e.read_write_ratio IS NULL OR e.read_write_ratio >= 1) THEN 2
        ELSE 3
    END,
    CONCAT(
        e.table_name, '.', e.name,
        '  reads=', CONVERT(VARCHAR(40), e.user_seeks + e.user_scans + e.user_lookups),
        '  updates=', CONVERT(VARCHAR(40), e.user_updates),
        '  r/w=', COALESCE(CAST(e.read_write_ratio AS VARCHAR(80)), 'inf'),
        '  seek_ratio=', COALESCE(CAST(e.seek_ratio AS VARCHAR(80)), 'n/a'),
        '  per_million=', COALESCE(CAST(e.reads_per_million_rows AS VARCHAR(80)), 'n/a')
    )
FROM #ExistingIndexes e

DECLARE @total_count INT
SELECT @total_count = COUNT(*) FROM #ExistingIndexes

PRINT '================================================================'
PRINT 'INDEX USAGE ANALYSIS - Kategorisering'
PRINT 'SQL Server version: ' + @ProductVersion + ' (major ' + CAST(@ProductMajorVersion AS VARCHAR(5)) + ')'
PRINT 'Server: ' + @@SERVERNAME + '  DB: ' + DB_NAME()
PRINT 'Metadata age: ' + CAST(@meta_age AS VARCHAR(10)) + ' dage'
       + CASE WHEN @meta_age < 14
              THEN '  (ADVARSEL: under 14 dage - DMV-tal er upaalidelige)'
              ELSE ''
         END
PRINT 'Total existing indexes analyseret: ' + CAST(@total_count AS VARCHAR(10))
PRINT '================================================================'
PRINT ''

DECLARE @categories TABLE (cat VARCHAR(20), ord INT, header NVARCHAR(150))
INSERT INTO @categories VALUES
    ('High impact',     1, '--- HIGH IMPACT (kritiske - behold/optimer) ---'),
    ('Good impact',     2, '--- GOOD IMPACT (solidt brugte - behold) ---'),
    ('Low impact',      3, '--- LOW IMPACT (lavt brug, reads > writes) ---'),
    ('Drop candidates', 4, '--- DROP CANDIDATES (writes > reads eller ubrugte) ---')

DECLARE @cur_cat VARCHAR(20), @cur_header NVARCHAR(150), @line_count INT
DECLARE @current_line NVARCHAR(MAX)

DECLARE cat_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT cat, header FROM @categories ORDER BY ord

OPEN cat_cursor
FETCH NEXT FROM cat_cursor INTO @cur_cat, @cur_header

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @line_count = COUNT(*) FROM @categorized WHERE category = @cur_cat

    PRINT @cur_header
    PRINT 'Antal: ' + CAST(@line_count AS VARCHAR(10))

    IF @line_count = 0
        PRINT '  (ingen)'
    ELSE
    BEGIN
        DECLARE line_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT line FROM @categorized
            WHERE category = @cur_cat
            ORDER BY line

        OPEN line_cursor
        FETCH NEXT FROM line_cursor INTO @current_line

        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT '  ' + @current_line
            FETCH NEXT FROM line_cursor INTO @current_line
        END

        CLOSE line_cursor
        DEALLOCATE line_cursor
    END
    PRINT ''

    FETCH NEXT FROM cat_cursor INTO @cur_cat, @cur_header
END

CLOSE cat_cursor
DEALLOCATE cat_cursor

PRINT '================================================================'

GOTO CLEANUP

-- =====================================================================
-- CLEANUP
-- =====================================================================
CLEANUP:
IF @drop_tmp_table = 1
BEGIN
    IF OBJECT_ID('tempdb..#MissingIndexes','U') IS NOT NULL DROP TABLE #MissingIndexes
    IF OBJECT_ID('tempdb..#ExistingIndexes','U') IS NOT NULL DROP TABLE #ExistingIndexes
    IF OBJECT_ID('tempdb..#Density_Selectability','U') IS NOT NULL DROP TABLE #Density_Selectability
    IF OBJECT_ID('tempdb..#useless_space_consumption','U') IS NOT NULL DROP TABLE #useless_space_consumption
END
