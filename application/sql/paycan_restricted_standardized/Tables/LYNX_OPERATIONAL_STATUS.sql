CREATE TABLE [paycan_restricted_standardized].[LYNX_OPERATIONAL_STATUS] (
    [LYNX_OPERATIONAL_STATUS_ID] BIGINT        NOT NULL,
    [BATCH_ID]                   INT           NOT NULL,
    [VALIDATION_STATUS]          BIT           NULL,
    [STATUS_LOG_ID]              BIGINT        NULL,
    [CYCLE_DATE]                 DATE          NULL,
    [INSTITUTION_NUMBER]         BIGINT        NULL,
    [INSTITUTION_IRN]            VARCHAR (255) NULL,
    [INSTITUTION_NAME]           VARCHAR (255) NULL,
    [PARTICIPANT_NUMBER]         BIGINT        NULL,
    [PARTICIPANT_BIC]            VARCHAR (255) NULL,
    [PARTICIPANT_NAME]           VARCHAR (255) NULL,
    [INSTITUTION]                VARCHAR (100) NULL,
    [PARTICIPANT]                VARCHAR (100) NULL,
    [STATUS_TYPE_CD]             VARCHAR (15)  NULL,
    [EFFECTIVE_TS]               DATETIME      NULL,
    [LAST_MODIFIED_TS]           DATETIME      NULL,
    [ingestion_date]             DATETIME      NULL,
    [batch_date]                 VARCHAR (10)  NULL,
    PRIMARY KEY CLUSTERED ([LYNX_OPERATIONAL_STATUS_ID] ASC, [BATCH_ID] ASC)
);

