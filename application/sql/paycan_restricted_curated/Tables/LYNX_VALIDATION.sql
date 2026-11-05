CREATE TABLE [paycan_restricted_curated].[LYNX_VALIDATION] (
    [BATCH_ID]        INT           NOT NULL,
    [FILE_NAME]       VARCHAR (255) NOT NULL,
    [FILE_PRIMARY_ID] BIGINT        NOT NULL,
    [BUSINESS_ID]     BIGINT        NULL,
    [FLAG_NAME]       VARCHAR (255) NOT NULL,
    [CREATETIME]      DATE          NULL,
    [ingestion_date]  DATETIME      NULL,
    [batch_date]      VARCHAR (10)  NULL,
    PRIMARY KEY CLUSTERED ([BATCH_ID] ASC, [FILE_NAME] ASC, [FILE_PRIMARY_ID] ASC, [FLAG_NAME] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_VALIDATION_batch_date]
    ON [paycan_restricted_curated].[LYNX_VALIDATION]([batch_date] ASC);

