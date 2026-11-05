-- select * from [paycan_restricted_curated].[Killed_process_log_table]    
-- select * from [paycan_restricted_curated].[LYNX_PAYMENT](nolock)

/*

DROP TABLE IF EXISTS [paycan_restricted_curated].[Killed_process_log_table]    
GO    
CREATE TABLE [paycan_restricted_curated].[Killed_process_log_table](    
  [ID] [int] IDENTITY(1,1) NOT NULL,    
  [SESSION_ID] [smallint] NOT NULL,    
  [TIME_RUNNING_SECONDS] [int] NULL,    
  [HOST_NAME] [nvarchar](128) NULL,    
  [USER_NAME] [nvarchar](128) NULL,    
  [STATUS] [nvarchar](30) NOT NULL,    
  [CPU_TIME] [int] NOT NULL,    
  [MEMORY_USAGE] [int] NOT NULL,    
  [COMMAND] [nvarchar](32) NULL,    
  [SQL_COMMAND] [nvarchar](max) NULL,    
  [DATABASE_ID] [smallint] NULL,    
  [DATABASE_NAME] [nvarchar](128) NULL,    
  [KILLED_AT] [datetime] NULL    
 PRIMARY KEY CLUSTERED     
 (    
  [ID] ASC    
 )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]    
 ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]  

*/



/****** Object:  StoredProcedure [paycan_restricted_curated].[SP_killLongRunningstatement]    Script Date: 03/01/2023 4:15:51 PM ******/
CREATE   PROCEDURE [paycan_restricted_curated].[SP_killLongRunningstatement]  
AS  
BEGIN  
  
/* Declare/Set max executoin time allowed */  
DECLARE @MAX_EXECUTION_TIME_IN_SECONDS INT  
DECLARE @SQL_CMD nvarchar(max)  
  
SET @MAX_EXECUTION_TIME_IN_SECONDS = 300 
  
DROP TABLE IF EXISTS [paycan_restricted_curated].[tbl_running_proces];  
  
 SELECT  
  [DES].SESSION_ID        AS [SESSION_ID],  
  DATEDIFF(second, [DER].start_time, getdate()) AS [TIME_RUNNING_SECONDS],  
  [DES].host_name         AS [HOST_NAME],  
  [DES].nt_user_name        AS [USER_NAME],  
  [DER].database_id        AS [DATABASE_ID],  
  db_name([DER].database_id)      AS [DATABASE_NAME],  
  [DER].status         AS [STATUS],  
  [DES].cpu_time         AS [CPU_TIME],  
  [DES].memory_usage        AS [MEMORY_USAGE],  
  [DER].command         AS [COMMAND],  
  sql_text.text         AS [SQL_COMMAND]   
 INTO [paycan_restricted_curated].[tbl_running_proces]  
 FROM SYS.DM_EXEC_SESSIONS [DES](NOLOCK)  
  LEFT JOIN SYS.DM_EXEC_REQUESTS [DER] (NOLOCK)   
   ON DES.SESSION_ID=DER.SESSION_ID  
  CROSS APPLY sys.dm_exec_sql_text(sql_handle) sql_text  
 WHERE   
  [DES].session_id > 50   AND  /* process under 50 are SQL server processes */  
  [DES].status  = 'running' AND  /* only running processes */  
  [DES].SESSION_ID <> @@SPID  AND  /* exclude the session running this statement */
  [DER].command in ('SELECT') AND  /* only select statement */
  DATEDIFF(second, [DER].start_time, getdate()) > @MAX_EXECUTION_TIME_IN_SECONDS  
  
  
  IF EXISTS(SELECT 1 FROM [paycan_restricted_curated].[tbl_running_proces])  
  BEGIN  
   /* Cursor variables */  
   DECLARE @SESSION_ID smallint  
   DECLARE @TIME_RUNNING_SECONDS int  
   DECLARE @HOST_NAME nvarchar(128)  
   DECLARE @USER_NAME nvarchar(128)  
   DECLARE @STATUS nvarchar(30)  
   DECLARE @CPU_TIME int  
   DECLARE @MEMORY_USAGE int  
   DECLARE @COMMAND nvarchar(32)  
   DECLARE @SQL_COMMAND nvarchar(max)  
   DECLARE @DATABASE_ID smallint  
   DECLARE @DATABASE_NAME nvarchar(128)  
  
  
   /* LOOP ON PROCESSES */  
   DECLARE cur_BusyProcess CURSOR FAST_FORWARD FOR  
  
    SELECT   
     [SESSION_ID], [TIME_RUNNING_SECONDS],  
     [HOST_NAME], [USER_NAME], [STATUS],  
     [CPU_TIME],  [MEMORY_USAGE], [COMMAND],  
     [SQL_COMMAND], [DATABASE_ID], [DATABASE_NAME]  
    FROM  
      [paycan_restricted_curated].[tbl_running_proces](NOLOCK)  
    WHERE  
     [TIME_RUNNING_SECONDS] >= @MAX_EXECUTION_TIME_IN_SECONDS  
     
   OPEN cur_BusyProcess  
   FETCH NEXT FROM cur_BusyProcess INTO  
  
     @SESSION_ID, @TIME_RUNNING_SECONDS,  
     @HOST_NAME,  @USER_NAME,  @STATUS,  
     @CPU_TIME,  @MEMORY_USAGE, @COMMAND,  
     @SQL_COMMAND, @DATABASE_ID, @DATABASE_NAME  
  
    WHILE ( @@FETCH_STATUS <> -1 )   
    BEGIN  
       
     SET @SQL_CMD = '';  
     SET @SQL_CMD = 'KILL ' +  CONVERT(VARCHAR,@SESSION_ID) + ';'  
     EXEC(@SQL_CMD)  
      
     INSERT INTO [paycan_restricted_curated].[Killed_process_log_table]  
     (  
      [SESSION_ID], [TIME_RUNNING_SECONDS],   
      [HOST_NAME], [USER_NAME], [STATUS],      
      [CPU_TIME],  [MEMORY_USAGE], [COMMAND],  
      [SQL_COMMAND], [DATABASE_ID], [DATABASE_NAME],    
      [KILLED_AT]  
     )  
     VALUES  
     (  
      @SESSION_ID, @TIME_RUNNING_SECONDS,   
      @HOST_NAME,  @USER_NAME,  @STATUS,  
      @CPU_TIME,  @MEMORY_USAGE, @COMMAND,  
      @SQL_COMMAND, @DATABASE_ID, @DATABASE_NAME,  
      getdate()  
     )  
  
    FETCH NEXT FROM cur_BusyProcess INTO  
  
     @SESSION_ID, @TIME_RUNNING_SECONDS,  
     @HOST_NAME,  @USER_NAME,  @STATUS,  
     @CPU_TIME,  @MEMORY_USAGE, @COMMAND,  
     @SQL_COMMAND, @DATABASE_ID, @DATABASE_NAME  
  
    END  
  
   CLOSE cur_BusyProcess   
   DEALLOCATE cur_BusyProcess  
 
  END

  DROP TABLE IF EXISTS [paycan_restricted_curated].[tbl_running_proces]; 

END
GO
GRANT EXECUTE
    ON OBJECT::[paycan_restricted_curated].[SP_killLongRunningstatement] TO [runspdevpaycan]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[paycan_restricted_curated].[SP_killLongRunningstatement] TO [runspact]
    AS [dbo];

