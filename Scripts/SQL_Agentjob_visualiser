--****************************************************************************************
-- This script returns a (graphical) timeline for all SQL jobs using google graph
--****************************************************************************************
-- Version: 1.1
-- Author:	Theo Ekelmans 
-- Modified by Jacob Saugmann
-- Email:	
-- Last modified date:	2021-10-28
--****************************************************************************************
SET NOCOUNT ON

DECLARE @DT datetime
DECLARE @StartDT datetime
DECLARE @EndDT datetime
DECLARE @MinRuntimeInSec TINYINT

DECLARE @jobnameFiler VARCHAR(200)
DECLARE @SaveAsFile TINYINT

--***************************************************************************************
-- Set variables
--***************************************************************************************
SET @StartDT = GETDATE() - 2
SET @EndDT = GETDATE()
SET @MinRuntimeInSec = 1 --Ignore jobs with runtime smaller then this
SET @jobnameFiler = '' -- leave as an empty '' if you want all jobs
SET @SaveAsFile = 0


--***************************************************************************************
-- Pre-run cleanup (just in case)
--***************************************************************************************
IF OBJECT_ID('tempdb..#JobRuntime') IS NOT NULL
	DROP TABLE #JobRuntime;
IF OBJECT_ID('tempdb..##GoogleGraph') IS NOT NULL
	DROP TABLE ##GoogleGraph;

--***************************************************************************************
-- Create a table for HTML assembly
--***************************************************************************************
CREATE TABLE ##GoogleGraph (
	[ID]   [int] IDENTITY(1,1) NOT NULL ,
	[HTML] [varchar](8000) NULL
)


--***************************************************************************************
-- Create the Job Runtime information table
--***************************************************************************************
SELECT job.name AS jobname,

	   CASE his.run_status WHEN 0 THEN 'Failed'
	                       WHEN 1 THEN 'Succeeded'
	                       WHEN 2 THEN 'Retry'
	                       WHEN 3 THEN 'Canceled'
	                       WHEN 4 THEN 'In Progress' END AS run_status,
	   his.run_status AS status_value,
	   run_duration,
	   CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120) AS SDT,
	   dateadd( s,
	((run_duration/10000)%100 * 3600) + ((run_duration/100)%100 * 60) + run_duration%100 ,
	CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120)
	) AS EDT
	INTO #JobRuntime
FROM msdb.dbo.sysjobs job               
	LEFT JOIN msdb.dbo.sysjobhistory his
			ON his.job_id = job.job_id

WHERE CONVERT(DATETIME, CONVERT(CHAR(8), run_date, 112) + ' ' + STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), run_time), 6), 5, 0, ':'), 3, 0, ':'), 120) BETWEEN @StartDT AND @EndDT
	AND step_id = 0 -- step_id = 0 is the entire job, step_id > 0 is actual step number
	AND ((run_duration/10000)%100 * 3600) + ((run_duration/100)%100 * 60) + run_duration%100 > @MinRuntimeInSec -- Ignore trivial runtimes
ORDER BY SDT



IF NOT EXISTS (SELECT 1
               FROM #JobRuntime)
	RETURN


IF @jobnameFiler <> ''
BEGIN
	DELETE FROM #JobRuntime
	WHERE jobname NOT LIKE @jobnameFiler
END

--***************************************************************************************
-- Format for google graph - Header
-- (Split into multiple inserts because the default text result setting is 256 chars)
--***************************************************************************************
INSERT INTO ##GoogleGraph ( HTML )
SELECT '<html>
	<head>
	<!--<META HTTP-EQUIV="refresh" CONTENT="3">-->
	<script type="text/javascript" src="https://www.google.com/jsapi?autoload={''modules'':[{''name'':''visualization'', ''packages'':[''timeline'']}]}"></script>'
INSERT INTO ##GoogleGraph ( HTML )
SELECT '    <script type="text/javascript">
	google.charts.load("current", { packages: ["timeline"], "language": "da" });
	google.charts.setOnLoadCallback(drawChart);
	function drawChart() {'
INSERT INTO ##GoogleGraph ( HTML )
SELECT '	var container = document.getElementById(''JobTimeline'');
	var chart = new google.visualization.Timeline(container);
	var dataTable = new google.visualization.DataTable();'
INSERT INTO ##GoogleGraph ( HTML )
SELECT '	dataTable.addColumn({ type: ''string'', id: ''Position'' });
	dataTable.addColumn({ type: ''date'', id: ''Start'' });
	dataTable.addColumn({ type: ''date'', id: ''End'' });	
	dataTable.addRows([
'

--***************************************************************************************
-- Format for google graph - Data
--***************************************************************************************
INSERT INTO ##GoogleGraph ( HTML )
SELECT '		[ '
	+'''' + jobname + ''', '

	+'new Date('
	+ cast(DATEPART(year , SDT) AS varchar(4))
	+', '+cast(DATEPART(month, SDT) -1 AS varchar(4)) --Java months count from 0
	+', '+cast(DATEPART(day, SDT) AS varchar(4))
	+', '+cast(DATEPART(hour, SDT) AS varchar(4))
	+', '+cast(DATEPART(minute, SDT) AS varchar(4))
	+', '+cast(DATEPART(second, SDT) AS varchar(4))
	+'), '

	+'new Date('
	+ cast(DATEPART(year, EDT) AS varchar(4))
	+', '+cast(DATEPART(month, EDT) -1 AS varchar(4)) --Java months count from 0
	+', '+cast(DATEPART(day, EDT) AS varchar(4))
	+', '+cast(DATEPART(hour, EDT) AS varchar(4))
	+', '+cast(DATEPART(minute, EDT) AS varchar(4))
	+', '+cast(DATEPART(second, EDT) AS varchar(4))
	+ ')],'
	 --+ char(10)
FROM #JobRuntime

--***************************************************************************************
-- Format for google graph - Footer
--***************************************************************************************
INSERT INTO ##GoogleGraph ( HTML )
SELECT '	]);

	var options = 
	{
		timeline: 	{ 
					groupByRowLabel: true,					
					rowLabelStyle: {fontName: ''Helvetica'', fontSize: 14 },
					barLabelStyle: {fontName: ''Helvetica'', fontSize: 14 }					
					},		

	};



	chart.draw(dataTable, options);

}'
INSERT INTO ##GoogleGraph ( HTML )
SELECT '
	</script>
	</head>
	<body>'
	+'<font face="Helvetica" size="3" >'
	+'<h2 style="color:red;">Job run example</h2> <h5>JOBLIST</h5>'	
	+'</br>'
	+'</font>
		<div id="JobTimeline" style="width: 1885px; height: 900px;"></div>'
	+'<p style="font-size:8pt; color:black;">Job timeline on: '+@@servername
	+' from '+CONVERT(varchar(20), @StartDT, 120)
	+' until '+CONVERT(varchar(20), @EndDT, 120)+'</p>'	
	+'</body>
</html>'


EXEC sp_configure 'show advanced options', 1;

RECONFIGURE;

EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;


DECLARE @sql varchar(8000)
SELECT @sql = 'bcp "set nocount on; SELECT html FROM ##GoogleGraph ORDER BY ID" queryout D:\joblist.html -c -t, -T -S' + @@servername
EXEC master..xp_cmdshell @sql


EXEC sp_configure 'show advanced options', '1'
RECONFIGURE
-- this disables xp_cmdshell
EXEC sp_configure 'xp_cmdshell', '0' 
RECONFIGURE



--***************************************************************************************
-- Cleanup
--***************************************************************************************

IF OBJECT_ID('tempdb..#JobRuntime') IS NOT NULL
	DROP TABLE #JobRuntime;
IF OBJECT_ID('tempdb..##GoogleGraph') IS NOT NULL
	DROP TABLE ##GoogleGraph;
