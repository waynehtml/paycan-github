CREATE TABLE [paycan_restricted_standardized].[LYNX_MINUM_BALANCE] (
    [LYNX_MINUM_BALANCE_ID]   BIGINT          NOT NULL,
    [BATCH_ID]                INT             NOT NULL,
    [VALIDATION_STATUS]       BIT             NULL,
    [CYCLE_DATE]              DATE            NULL,
    [INSTITUTION_NUMBER]      BIGINT          NULL,
    [INSTITUTION_IRN]         VARCHAR (255)   NULL,
    [INSTITUTION_NAME]        VARCHAR (255)   NULL,
    [PARTICIPANT_NUMBER]      BIGINT          NULL,
    [PARTICIPANT_BIC]         VARCHAR (255)   NULL,
    [PARTICIPANT_NAME]        VARCHAR (255)   NULL,
    [INSTITUTION]             VARCHAR (100)   NULL,
    [PARTICIPANT]             VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD] VARCHAR (25)    NULL,
    [MINIMUM_AMOUNT]          DECIMAL (30, 2) NULL,
    [MET_TS]                  DATETIME        NULL,
    [LAST_MODIFIED_TS]        DATETIME        NULL,
    [ingestion_date]          DATETIME        NULL,
    [batch_date]              VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_MINUM_BALANCE_ID] ASC, [BATCH_ID] ASC)
);

