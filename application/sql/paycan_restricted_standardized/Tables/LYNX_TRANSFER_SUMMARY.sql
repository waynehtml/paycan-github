CREATE TABLE [paycan_restricted_standardized].[LYNX_TRANSFER_SUMMARY] (
    [BATCH_ID]            INT             NOT NULL,
    [TRANSFER_ID]         BIGINT          NOT NULL,
    [PAYER_REFERENCE]     VARCHAR (100)   NULL,
    [CYCLE_DATE]          DATE            NULL,
    [INSTITUTION]         VARCHAR (100)   NULL,
    [PARTICIPANT]         VARCHAR (100)   NULL,
    [FROM_ACCOUNT_BBAN]   VARCHAR (100)   NULL,
    [FROM_ACCOUNT_NAME]   VARCHAR (100)   NULL,
    [FROM_SETTLE_MECH_CD] VARCHAR (100)   NULL,
    [TO_ACCOUNT_BBAN]     VARCHAR (100)   NULL,
    [TO_ACCOUNT_NAME]     VARCHAR (100)   NULL,
    [TO_SETTLE_MECH_CD]   VARCHAR (100)   NULL,
    [TRANSACTION_TYPE]    VARCHAR (100)   NULL,
    [AMOUNT]              DECIMAL (22, 2) NULL,
    [REGISTERED]          TINYINT         NULL,
    [SETTLED]             TINYINT         NULL,
    [REGISTERED_TIME]     DATETIME        NULL,
    [SETTLED_TIME]        DATETIME        NULL,
    [ingestion_date]      DATETIME        NULL,
    [batch_date]          VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([BATCH_ID] ASC, [TRANSFER_ID] ASC)
);

