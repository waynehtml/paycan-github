CREATE TABLE [paycan_restricted_curated].[LYNX_GRIDLOCK] (
    [LYNX_GRIDLOCK_ID]        BIGINT          NOT NULL,
    [BATCH_ID]                INT             NOT NULL,
    [VALIDATION_STATUS]       BIT             NULL,
    [GRIDLOCK_PARTICIPANT_ID] BIGINT          NULL,
    [GRIDLOCK_BATCH_ID]       BIGINT          NULL,
    [CYCLE_DATE]              DATE            NULL,
    [START_TS]                DATETIME        NULL,
    [END_TS]                  DATETIME        NULL,
    [TRIGGER]                 VARCHAR (20)    NULL,
    [GRIDLOCK_INTERVAL]       BIGINT          NULL,
    [INSTITUTION_NUMBER]      BIGINT          NULL,
    [INSTITUTION_IRN]         VARCHAR (255)   NULL,
    [INSTITUTION_NAME]        VARCHAR (255)   NULL,
    [PARTICIPANT_NUMBER]      BIGINT          NULL,
    [PARTICIPANT_BIC]         VARCHAR (255)   NULL,
    [PARTICIPANT_NAME]        VARCHAR (255)   NULL,
    [INSTITUTION]             VARCHAR (100)   NULL,
    [PARTICIPANT]             VARCHAR (100)   NULL,
    [BEGIN_QUEUE_VALUE]       DECIMAL (30, 2) NULL,
    [END_QUEUE_VALUE]         DECIMAL (30, 2) NULL,
    [BEGIN_QUEUE_VOLUME]      BIGINT          NULL,
    [END_QUEUE_VOLUME]        BIGINT          NULL,
    [LAST_MODIFIED_TS]        DATETIME        NULL,
    [MATCHED_WITH_PAY]        CHAR (1)        NULL,
    [ingestion_date]          DATETIME        NULL,
    [batch_date]              VARCHAR (10)    NULL,
    PRIMARY KEY CLUSTERED ([LYNX_GRIDLOCK_ID] ASC, [BATCH_ID] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IDX_LYNX_GRIDLOCK_batch_date]
    ON [paycan_restricted_curated].[LYNX_GRIDLOCK]([batch_date] ASC);

