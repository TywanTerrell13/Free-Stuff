

/**************************************************************************
Author Tywan Terrell
object type: Stored Procedure
Description: This procedure was created to find the average start and end 
			 times for SQL Agent Jobs. This prcedure take one parameter
			 @reporttype.
			 
			 @reporttype = 1 -> Monthly average by job.
			 @reporttype = 2 -> Monthly average by Step  

			 Example Call
			 exec MonthlyJobAvgTimes @reporttype = 1
			 go;
			 exec MonthlyJobAvgTimes @reporttype = 2

***************************************************************************/
go;
Create Procedure MonthlyJobAvgTimes( @reporttype int)
as
begin

if @reporttype = 1 
begin
IF OBJECT_ID('tempdb..#MONTHLYJOBAVGTIME') IS NOT NULL
    DROP TABLE #MONTHLYJOBAVGTIME

;WITH JOBHISTORY AS
(
 SELECT sj.name,
        sh.run_date,
        sh.step_name,
		CAST(CONVERT(VARCHAR(10),RUN_DATE)AS DATE) AS 'FORMATEDDATE',
        STUFF(STUFF(RIGHT(REPLICATE('0', 6) +  CAST(sh.run_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':') 'run_time',
        STUFF(STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(sh.run_duration as varchar(8)), 8), 3, 0, ':'), 6, 0, ':'), 9, 0, ':') 'run_duration (DD:HH:MM:SS)  '
		
FROM msdb.dbo.sysjobs sj
JOIN msdb.dbo.sysjobhistory sh ON sj.job_id = sh.job_id
)
, JOBHISTORYSECONDS AS (

SELECT *, SUBSTRING(CONVERT (VARCHAR,FORMATEDDATE),6,2) AS MONTHNUMBER	, substring(cast (FORMATEDDATE as varchar),1,8) +'01 00:00:00'	as FOD
		,( SUBSTRING(RUN_TIME,1,2) *3600)+ (SUBSTRING(RUN_TIME,4,2)* 60) +SUBSTRING(RUN_TIME,7,2) AS 'RUN_START_TIME(SEC)'
		, (SUBSTRING([run_duration (DD:HH:MM:SS)  ],1,2)*3600*24)+ (SUBSTRING([run_duration (DD:HH:MM:SS)  ],4,2)*3600)+(SUBSTRING([run_duration (DD:HH:MM:SS)  ],7,2)*60)+SUBSTRING([run_duration (DD:HH:MM:SS)  ],10,2) AS [run_duration_SEC ]
FROM JOBHISTORY  
)
, MONTHLYJOBAVGTIME AS (

SELECT 
name,MONTHNUMBER, FOD ,AVG([RUN_START_TIME(SEC)])  AS M_AVG_START_SEC,AVG([run_duration_SEC ]) AS M_AVG_DURATION
FROM JOBHISTORYSECONDS JS
GROUP BY 
JS.name,JS.MONTHNUMBER,FOD
)

SELECT 'MONTHLY-AVERAGE' AS AVG_TYPE,*, DATEADD(S,M_AVG_START_SEC, FOD )AS M_AVG_START_TIME, DATEADD(S,M_AVG_DURATION, DATEADD(S,M_AVG_START_SEC, FOD )) AS M_AVG_END_TIME
INTO #MONTHLYJOBAVGTIME
FROM 
	MONTHLYJOBAVGTIME



sELECT name,AvgDesc, [1] AS Jan, [2] as Feb,[3] as Mar,[4] as Apr,[5] as May,[6] as Jun,[7]as Jul,[8] as Aug,[9] As Sept,[10] as Oct,[11] as NOv,[12] as [Dec] 
FROM 
(SELECT name ,'StartTime' as AvgDesc,  CAST(MONTHNUMBER AS INT) AS MONTHNUMBER , (M_AVG_START_TIME)as M_AVG_START_TIME
	 FROM #MONTHLYJOBAVGTIME) AS SOURCEPIVOT
PIVOT
(
	MAX(M_AVG_START_TIME)
	FOR MONTHNUMBER IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12] )
) AS PIVOTTABLE
union all 
--Endtime Pivot 
sELECT name,AvgDesc, [1] AS Jan, [2] as Feb,[3] as Mar,[4] as Apr,[5] as May,[6] as Jun,[7]as Jul,[8] as Aug,[9] As Sept,[10] as Oct,[11] as NOv,[12] as [Dec] 
FROM 
(SELECT name ,'EndTime' as AvgDesc,  CAST(MONTHNUMBER AS INT) AS MONTHNUMBER ,  M_AVG_END_TIME FROM #MONTHLYJOBAVGTIME) AS SOURCEPIVOT
PIVOT
(
	MAX(M_AVG_END_TIME)
	FOR MONTHNUMBER IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12] )
) AS PIVOTTABLE
order by 1
end

if @reporttype = 2
begin
--

IF OBJECT_ID('tempdb..#MONTHLYJOB_STEPAVGTIME') IS NOT NULL
    DROP TABLE #MONTHLYJOB_STEPAVGTIME


;WITH JOBHISTORY AS
(
 SELECT sj.name,
        sh.run_date,
        sh.step_name,
		CAST(CONVERT(VARCHAR(10),RUN_DATE)AS DATE) AS 'FORMATEDDATE',
        STUFF(STUFF(RIGHT(REPLICATE('0', 6) +  CAST(sh.run_time as varchar(6)), 6), 3, 0, ':'), 6, 0, ':') 'run_time',
        STUFF(STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(sh.run_duration as varchar(8)), 8), 3, 0, ':'), 6, 0, ':'), 9, 0, ':') 'run_duration (DD:HH:MM:SS)  '
		
FROM msdb.dbo.sysjobs sj
	
inner JOIN msdb.dbo.sysjobhistory sh ON sj.job_id = sh.job_id
)
, JOBHISTORYSECONDS AS (

SELECT *, SUBSTRING(CONVERT (VARCHAR,FORMATEDDATE),6,2) AS MONTHNUMBER	, substring(cast (FORMATEDDATE as varchar),1,8) +'01 00:00:00'	as FOD	
		,( SUBSTRING(RUN_TIME,1,2) *3600)+ (SUBSTRING(RUN_TIME,4,2)* 60) +SUBSTRING(RUN_TIME,7,2) AS 'RUN_START_TIME(SEC)'
		, (SUBSTRING([run_duration (DD:HH:MM:SS)  ],1,2)*3600*24)+ (SUBSTRING([run_duration (DD:HH:MM:SS)  ],4,2)*3600)+(SUBSTRING([run_duration (DD:HH:MM:SS)  ],7,2)*60)+SUBSTRING([run_duration (DD:HH:MM:SS)  ],10,2) AS [run_duration_SEC ]
FROM JOBHISTORY  
)
, MONTHLYJOB_STEPAVGTIME AS (

SELECT 
	name,step_name,FOD,MONTHNUMBER,AVG([RUN_START_TIME(SEC)])  AS M_AVG_START_SEC,AVG([run_duration_SEC ]) AS M_AVG_DURATION
FROM JOBHISTORYSECONDS JS
GROUP BY 
JS.name,step_name,FOD,JS.MONTHNUMBER
)

SELECT 'MONTHLY STEP AVERAGE' AS AVG_TYPE,*, DATEADD(S,M_AVG_START_SEC, FOD ) AS M_AVG_START_TIME, DATEADD(S,M_AVG_DURATION, DATEADD(S,M_AVG_START_SEC, FOD )) AS M_AVG_END_TIME
      into #MONTHLYJOB_STEPAVGTIME
FROM 
	MONTHLYJOB_STEPAVGTIME




SELECT name,step_name,TimeType, [1] AS Jan, [2] as Feb,[3] as Mar,[4] as Apr,[5] as May,[6] as Jun,[7]as Jul,[8] as Aug,[9] As Sept,[10] as Oct,[11] as NOv,[12] as [Dec] 
FROM 
(SELECT name ,step_name,'StartTime' as TimeType,  CAST(MONTHNUMBER AS INT) AS MONTHNUMBER , replace(M_AVG_START_TIME,'1900-01-01','')as M_AVG_START_TIME
	 FROM #MONTHLYJOB_STEPAVGTIME) AS SOURCEPIVOT
PIVOT
(
	MAX(M_AVG_START_TIME)
	FOR MONTHNUMBER IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12] )
) AS PIVOTTABLE
union all 
--Endtime Pivot 
sELECT name,step_name,TimeType, [1] AS Jan, [2] as Feb,[3] as Mar,[4] as Apr,[5] as May,[6] as Jun,[7]as Jul,[8] as Aug,[9] As Sept,[10] as Oct,[11] as Nov,[12] as [Dec] 
FROM 
(SELECT name ,step_name,'EndTime' as TimeType,  CAST(MONTHNUMBER AS INT) AS MONTHNUMBER ,  M_AVG_END_TIME FROM #MONTHLYJOB_STEPAVGTIME) AS SOURCEPIVOT
PIVOT
(
	MAX(M_AVG_END_TIME)
	FOR MONTHNUMBER IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12] )--, 2,3,4,5,6,7,8,9,10,11,12)
) AS PIVOTTABLE
order by name,step_name,TimeType desc 
end 
end