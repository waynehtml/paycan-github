CREATE TABLE [paycan_byod].[LYNX_CREDIT_POSITION] (
    [LYNX_CREDIT_POSITION_ID] BIGINT          NOT NULL,
    [BATCH_ID]                INT             NOT NULL,
    [VALIDATION_STATUS]       BIT             NULL,
    [CREDIT_ENTRY_ID]         BIGINT          NULL,
    [CYCLE_DATE]              DATE            NULL,
    [INSTITUTION]             VARCHAR (100)   NULL,
    [PARTICIPANT]             VARCHAR (10)    NULL,
    [EVENT_TS]                DATETIME        NULL,
    [TRIGGER_CD]              VARCHAR (15)    NULL,
    [TRIGGER_NAME]            VARCHAR (100)   NULL,
    [TOTAL_LIMIT]             DECIMAL (22, 2) NULL,
    [TOTAL_AVAILABLE]         DECIMAL (22, 2) NULL,
    [ENTRY_TYPE_CD]           VARCHAR (15)    NULL,
    [ENTRY_TYPE_NAME]         VARCHAR (100)   NULL,
    [ENTRY_AMOUNT]            DECIMAL (22, 2) NULL,
    [LAST_MODIFIED_TS]        DATETIME        NULL,
    PRIMARY KEY CLUSTERED ([LYNX_CREDIT_POSITION_ID] ASC, [BATCH_ID] ASC)
);

