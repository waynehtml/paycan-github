CREATE TABLE [paycan_restricted_curated].[LYNX_TRANSFER] (
    [LYNX_TRANSFER_ID]            BIGINT          NOT NULL,
    [BATCH_ID]                    INT             NOT NULL,
    [VALIDATION_STATUS]           BIT             NULL,
    [TRANSFER_ID]                 BIGINT          NULL,
    [SYSTEM_REFERENCE]            VARCHAR (255)   NULL,
    [PAYER_REFERENCE]             VARCHAR (50)    NULL,
    [CYCLE_DATE]                  DATE            NULL,
    [INSTITUTION_NUMBER]          BIGINT          NULL,
    [INSTITUTION_IRN]             VARCHAR (255)   NULL,
    [INSTITUTION_NAME]            VARCHAR (255)   NULL,
    [PARTICIPANT_NUMBER]          BIGINT          NULL,
    [PARTICIPANT_BIC]             VARCHAR (255)   NULL,
    [PARTICIPANT_NAME]            VARCHAR (255)   NULL,
    [INSTITUTION]                 VARCHAR (100)   NULL,
    [PARTICIPANT]                 VARCHAR (100)   NULL,
    [FROM_ACCOUNT_ID]             BIGINT          NULL,
    [FROM_ACCOUNT_BBAN]           VARCHAR (20)    NULL,
    [FROM_ACCOUNT_NAME]           VARCHAR (20)    NULL,
    [FROM_SETTLE_MECH_CD]         VARCHAR (20)    NULL,
    [TO_ACCOUNT_ID]               BIGINT          NULL,
    [TO_ACCOUNT_BBAN]             VARCHAR (20)    NULL,
    [TO_ACCOUNT_NAME]             VARCHAR (255)   NULL,
    [TO_SETTLE_MECH_CD]           VARCHAR (20)    NULL,
    [TRANSACTION_TYPE]            VARCHAR (100)   NULL,
    [AMOUNT]                      DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]            DATETIME        NULL,
    [STATUS_TYPE_CD]              VARCHAR (15)    NULL,
    [STATUS_CHANGE_TS]            DATETIME        NULL,
    [STATUS_REASON]               VARCHAR (255)   NULL,
    [STATUS_MILLISECOND_DURATION] BIGINT          NULL,
    [ingestion_date]              DATETIME        NULL,
    [batch_date]                  VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_TRANSFER_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_TRANSFER_batch_date]
    ON [paycan_restricted_curated].[LYNX_TRANSFER]([batch_date] ASC);

