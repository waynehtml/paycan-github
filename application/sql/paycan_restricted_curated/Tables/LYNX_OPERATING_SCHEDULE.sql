CREATE TABLE [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE] (
    [LYNX_OPERATING_SCHEDULE_ID] BIGINT        NOT NULL,
    [BATCH_ID]                   INT           NOT NULL,
    [VALIDATION_STATUS]          BIT           NULL,
    [WINDOW_LOG_ID]              BIGINT        NULL,
    [CYCLE_DATE]                 DATE          NULL,
    [EVENT]                      VARCHAR (100) NULL,
    [START_TS]                   DATETIME      NULL,
    [END_TS]                     DATETIME      NULL,
    [LAST_MODIFIED_TS]           DATETIME      NULL,
    [DURATION]                   BIGINT        NULL,
    [ingestion_date]             DATETIME      NULL,
    [batch_date]                 VARCHAR (10)  NULL,
    PRIMARY KEY CLUSTERED ([LYNX_OPERATING_SCHEDULE_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_OPERATING_SCHEDULE_batch_date]
    ON [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE]([batch_date] ASC);

