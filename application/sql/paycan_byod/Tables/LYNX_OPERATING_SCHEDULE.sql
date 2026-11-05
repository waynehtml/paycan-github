CREATE TABLE [paycan_byod].[LYNX_OPERATING_SCHEDULE] (
    [LYNX_OPERATING_SCHEDULE_ID] BIGINT       NOT NULL,
    [BATCH_ID]                   INT          NOT NULL,
    [VALIDATION_STATUS]          BIT          NULL,
    [WINDOW_LOG_ID]              BIGINT       NULL,
    [CYCLE_DATE]                 DATE         NULL,
    [EVENT]                      VARCHAR (25) NULL,
    [START_TS]                   DATETIME     NULL,
    [END_TS]                     DATETIME     NULL,
    [LAST_MODIFIED_TS]           DATETIME     NULL,
    [DURATION]                   BIGINT       NULL,
    PRIMARY KEY CLUSTERED ([LYNX_OPERATING_SCHEDULE_ID] ASC, [BATCH_ID] ASC)
);

