CREATE TABLE [paycan_byod].[LYNX_LOAN] (
    [LYNX_LOAN_ID]      BIGINT          NOT NULL,
    [BATCH_ID]          INT             NOT NULL,
    [VALIDATION_STATUS] BIT             NULL,
    [LOAN_ENTRY_ID]     BIGINT          NULL,
    [CYCLE_DATE]        DATE            NULL,
    [INSTITUTION]       VARCHAR (100)   NULL,
    [PARTICIPANT]       VARCHAR (100)   NULL,
    [ACCOUNT_NAME]      VARCHAR (10)    NULL,
    [ENTRY_TYPE]        VARCHAR (25)    NULL,
    [AMOUNT]            DECIMAL (22, 2) NULL,
    [ENTRY_TS]          DATETIME        NULL,
    [ACCOUNT_BALANCE]   DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]  DATETIME        NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LOAN_ID] ASC, [BATCH_ID] ASC)
);

