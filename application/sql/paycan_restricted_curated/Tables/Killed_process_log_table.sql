CREATE TABLE [paycan_restricted_curated].[Killed_process_log_table] (
    [ID]                   INT            IDENTITY (1, 1) NOT NULL,
    [SESSION_ID]           SMALLINT       NOT NULL,
    [TIME_RUNNING_SECONDS] INT            NULL,
    [HOST_NAME]            NVARCHAR (128) NULL,
    [USER_NAME]            NVARCHAR (128) NULL,
    [STATUS]               NVARCHAR (30)  NOT NULL,
    [CPU_TIME]             INT            NOT NULL,
    [MEMORY_USAGE]         INT            NOT NULL,
    [COMMAND]              NVARCHAR (32)  NULL,
    [SQL_COMMAND]          NVARCHAR (MAX) NULL,
    [DATABASE_ID]          SMALLINT       NULL,
    [DATABASE_NAME]        NVARCHAR (128) NULL,
    [KILLED_AT]            DATETIME       NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC)
);

