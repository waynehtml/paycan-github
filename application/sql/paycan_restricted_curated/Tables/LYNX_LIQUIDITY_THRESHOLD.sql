CREATE TABLE [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD] (
    [LYNX_LIQUIDITY_THRESHOLD_ID] BIGINT          NOT NULL,
    [BATCH_ID]                    INT             NOT NULL,
    [VALIDATION_STATUS]           BIT             NULL,
    [CREATE_DATE]                 DATE            NULL,
    [ACTIVE_DATE]                 DATE            NULL,
    [DELETE_DATE]                 DATE            NULL,
    [CHANGE_TS]                   DATETIME        NULL,
    [INSTITUTION_NUMBER]          BIGINT          NULL,
    [INSTITUTION_IRN]             VARCHAR (255)   NULL,
    [INSTITUTION_NAME]            VARCHAR (255)   NULL,
    [PARTICIPANT_NUMBER]          BIGINT          NULL,
    [PARTICIPANT_BIC]             VARCHAR (255)   NULL,
    [PARTICIPANT_NAME]            VARCHAR (255)   NULL,
    [INSTITUTION]                 VARCHAR (100)   NULL,
    [PARTICIPANT]                 VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD]     VARCHAR (20)    NULL,
    [THRESHOLD_AMOUNT]            DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]            DATETIME        NULL,
    [ingestion_date]              DATETIME        NULL,
    [batch_date]                  VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LIQUIDITY_THRESHOLD_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_LIQUIDITY_THRESHOLD_batch_date]
    ON [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD]([batch_date] ASC);

