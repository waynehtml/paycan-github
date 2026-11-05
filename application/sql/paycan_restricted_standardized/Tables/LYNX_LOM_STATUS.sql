CREATE TABLE [paycan_restricted_standardized].[LYNX_LOM_STATUS] (
    [LYNX_LOM_STATUS_ID]      BIGINT        NOT NULL,
    [BATCH_ID]                INT           NOT NULL,
    [VALIDATION_STATUS]       BIT           NULL,
    [CYCLE_DATE]              DATE          NULL,
    [INSTITUTION_NUMBER]      BIGINT        NULL,
    [INSTITUTION_IRN]         VARCHAR (255) NULL,
    [INSTITUTION_NAME]        VARCHAR (255) NULL,
    [PARTICIPANT_NUMBER]      BIGINT        NULL,
    [PARTICIPANT_BIC]         VARCHAR (255) NULL,
    [PARTICIPANT_NAME]        VARCHAR (255) NULL,
    [INSTITUTION]             VARCHAR (100) NULL,
    [PARTICIPANT]             VARCHAR (100) NULL,
    [SETTLEMENT_MECHANISM_CD] VARCHAR (20)  NULL,
    [STATUS_CD]               VARCHAR (10)  NULL,
    [EFFECTIVE_TS]            DATETIME      NULL,
    [LAST_MODIFIED_TS]        DATETIME      NULL,
    [ingestion_date]          DATETIME      NULL,
    [batch_date]              VARCHAR (10)  NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LOM_STATUS_ID] ASC, [BATCH_ID] ASC)
);

