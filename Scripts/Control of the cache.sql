-- =============================================
-- Author:      Jacob Saugmann
-- Create date: 2023-03-21 05:37:52
-- Description: Script to show how many 1 use plans are in the cache
--				debug=1 only outputs the results no action is taken
-- =============================================


SET NOCOUNT ON;

DECLARE @debug BIT = 1


DROP TABLE IF EXISTS #cache

IF @debug = 1
BEGIN
	RAISERROR ('--| DEBUG: Running in debug mode, will only output the results, no action wil be taken', 0, 1) WITH NOWAIT
END


SELECT
	objtype AS cachetype,
	COUNT_BIG(*) AS total_plans,
	SUM(CAST(size_in_bytes AS DECIMAL(18, 2))) / 1024 / 1024 AS total_mbs,
	AVG(usecounts) AS avg_use_count,
	SUM(CAST((
	CASE
		 WHEN usecounts = 1 THEN size_in_bytes
		 ELSE 0
	END
	) AS DECIMAL(18, 2))) / 1024 / 1024 AS total_mbs_use_count_1,
	SUM(CASE
		 WHEN usecounts = 1 THEN 1
		 ELSE 0
	END) AS total_plans_use_count_1
INTO #cache
FROM sys.dm_exec_cached_plans
GROUP BY objtype

DECLARE @cachesize INT, @total_plans DECIMAL(8, 2), @one_use_pct DECIMAL(5, 2)

SELECT
	@cachesize = SUM(Total_MBs_use_count_1),
	@total_plans = SUM(total_plans_use_count_1)
FROM #cache
WHERE CacheType IN ('Prepared', 'Adhoc')


SELECT
	@one_use_pct = (@total_plans / SUM(total_plans)) * 100.00
FROM #cache c

IF @debug = 1
BEGIN
	SELECT
		*
	FROM #cache AS c
	ORDER BY c.total_mbs DESC
END


DROP TABLE IF EXISTS #cache

--Variables for text output
DECLARE @cache_txt VARCHAR(25)
DECLARE @cache_pct_txt VARCHAR(25)

IF (@cachesize > 5000 OR
@one_use_pct > 50.0)
BEGIN
	SET @cache_txt = CAST(@cachesize AS VARCHAR(25))
	SET @cache_pct_txt = CAST(@one_use_pct AS VARCHAR(25))
	RAISERROR ('The cache holds a large amount of 1 use plans : %s mb | %s %% of cache, cleaning the cache', 0, 1, @cache_txt, @cache_pct_txt) WITH LOG

	IF @debug = 0
	BEGIN
		DBCC FREESYSTEMCACHE ('SQL Plans');
	END

END
ELSE
BEGIN

RAISERROR('--| INFO: The cache seems not bloaded with 1 use plans, no actions was taken',0,1) WITH NOWAIT	

END

SET @cache_txt = CAST(@cachesize AS VARCHAR(25))
SET @cache_pct_txt = CAST(@one_use_pct AS VARCHAR(25))
RAISERROR ('--| INFO: The cache holds %s mb of 1 use plans | That makes it  %s %% of the cache.' , 0, 1, @cache_txt, @cache_pct_txt)  WITH NOWAIT


