CREATE TABLE [paycan_byod].[LYNX_OPERATIONAL_STATUS] (
    [LYNX_OPERATIONAL_STATUS_ID] BIGINT        NOT NULL,
    [BATCH_ID]                   INT           NOT NULL,
    [VALIDATION_STATUS]          BIT           NULL,
    [STATUS_LOG_ID]              BIGINT        NULL,
    [CYCLE_DATE]                 DATE          NULL,
    [INSTITUTION]                VARCHAR (100) NULL,
    [PARTICIPANT]                VARCHAR (100) NULL,
    [STATUS_TYPE_CD]             VARCHAR (15)  NULL,
    [EFFECTIVE_TS]               DATETIME      NULL,
    [LAST_MODIFIED_TS]           DATETIME      NULL,
    PRIMARY KEY CLUSTERED ([LYNX_OPERATIONAL_STATUS_ID] ASC, [BATCH_ID] ASC)
);

