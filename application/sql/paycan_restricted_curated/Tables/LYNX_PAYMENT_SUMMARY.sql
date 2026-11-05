CREATE TABLE [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY] (
    [BATCH_ID]                INT             NOT NULL,
    [PAYMENT_ID]              BIGINT          NOT NULL,
    [PAYER_REFERENCE]         VARCHAR (100)   NULL,
    [SYSTEM_REFERENCE]        VARCHAR (100)   NULL,
    [CYCLE_DATE]              DATE            NULL,
    [PAYER_INSTITUTION]       VARCHAR (100)   NULL,
    [PAYER_PARTICIPANT]       VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD] VARCHAR (100)   NULL,
    [RECEIVER_INSTITUTION]    VARCHAR (100)   NULL,
    [RECEIVER_PARTICIPANT]    VARCHAR (100)   NULL,
    [SWIFT_MSG_TYPE_CD]       VARCHAR (100)   NULL,
    [TRANSACTION_TYPE]        VARCHAR (100)   NULL,
    [AMOUNT]                  DECIMAL (22, 2) NULL,
    [GRIDLOCK_SETTLED_ID]     BIGINT          NULL,
    [PRIORITY]                BIGINT          NULL,
    [REGISTERED]              TINYINT         NULL,
    [SETTLED]                 TINYINT         NULL,
    [QUEUED]                  TINYINT         NULL,
    [DIARISED]                TINYINT         NULL,
    [REMOVED]                 TINYINT         NULL,
    [REPRIORITISE]            TINYINT         NULL,
    [DISCARDED]               TINYINT         NULL,
    [RESEQUENCED]             INT             NULL,
    [INSUFFUNDS]              TINYINT         NULL,
    [REGISTERED_TIME]         DATETIME        NULL,
    [SETTLED_TIME]            DATETIME        NULL,
    [QUEUED_TIME]             DATETIME        NULL,
    [DIARISED_TIME]           DATETIME        NULL,
    [REMOVED_TIME]            DATETIME        NULL,
    [REPRIORITISE_TIME]       DATETIME        NULL,
    [DISCARDED_TIME]          DATETIME        NULL,
    [RESEQUENCED_TIME]        DATETIME        NULL,
    [INSUFFUNDS_TIME]         DATETIME        NULL,
    [ingestion_date]          DATETIME        NULL,
    [batch_date]              VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([BATCH_ID] ASC, [PAYMENT_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_PAYMENT_SUMMARY_batch_date]
    ON [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY]([batch_date] ASC);

