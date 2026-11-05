CREATE TABLE [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY] (
    [LYNX_CREDIT_ACTIVITY_ID] BIGINT          NOT NULL,
    [BATCH_ID]                INT             NOT NULL,
    [VALIDATION_STATUS]       BIT             NULL,
    [CREDIT_ACTIVITY_ID]      BIGINT          NULL,
    [CYCLE_DATE]              DATE            NULL,
    [INSTITUTION_NUMBER]      BIGINT          NULL,
    [INSTITUTION_IRN]         VARCHAR (255)   NULL,
    [INSTITUTION_NAME]        VARCHAR (255)   NULL,
    [PARTICIPANT_NUMBER]      BIGINT          NULL,
    [PARTICIPANT_BIC]         VARCHAR (255)   NULL,
    [PARTICIPANT_NAME]        VARCHAR (255)   NULL,
    [INSTITUTION]             VARCHAR (100)   NULL,
    [PARTICIPANT]             VARCHAR (100)   NULL,
    [SYSTEM_REFERENCE_CD]     VARCHAR (100)   NULL,
    [TOTAL_AMOUNT]            DECIMAL (22, 2) NULL,
    [EARMARKED_AMOUNT]        DECIMAL (22, 2) NULL,
    [EARMARKED_LOM]           VARCHAR (25)    NULL,
    [LAST_MODIFIED_TS]        DATETIME        NULL,
    [STATUS_TYPE_CD]          VARCHAR (15)    NULL,
    [STATUS_CHANGE_TIME]      DATETIME        NULL,
    [STATUS_REASON_CD]        VARCHAR (50)    NULL,
    [STATUS_TYPE_CATEGORY]    VARCHAR (255)   NULL,
    [ingestion_date]          DATETIME        NULL,
    [batch_date]              VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_CREDIT_ACTIVITY_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_CREDIT_ACTIVITY_batch_date]
    ON [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY]([batch_date] ASC);

