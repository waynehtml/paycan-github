CREATE TABLE [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION] (
    [LYNX_LIQUIDITY_POSITION_ID] BIGINT          NOT NULL,
    [BATCH_ID]                   INT             NOT NULL,
    [VALIDATION_STATUS]          BIT             NULL,
    [CYCLE_DATE]                 DATE            NULL,
    [INSTITUTION_NUMBER]         BIGINT          NULL,
    [INSTITUTION_IRN]            VARCHAR (255)   NULL,
    [INSTITUTION_NAME]           VARCHAR (255)   NULL,
    [INSTITUTION]                VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD]    VARCHAR (20)    NULL,
    [SENT_AMOUNT]                DECIMAL (30, 2) NULL,
    [RECEIVED_AMOUNT]            DECIMAL (30, 2) NULL,
    [LIQUIDITY_POSITION]         DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]           DATETIME        NULL,
    [ingestion_date]             DATETIME        NULL,
    [batch_date]                 VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LIQUIDITY_POSITION_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_LIQUIDITY_POSITION_batch_date]
    ON [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION]([batch_date] ASC);

