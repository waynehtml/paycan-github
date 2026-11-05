CREATE TABLE [paycan_byod].[LYNX_NET_SEND_LIMIT] (
    [LYNX_NET_SEND_LIMIT_ID]  BIGINT          NOT NULL,
    [BATCH_ID]                INT             NOT NULL,
    [VALIDATION_STATUS]       BIT             NULL,
    [NET_SEND_LIMIT_ID]       BIGINT          NULL,
    [CYCLE_DATE]              DATE            NULL,
    [REGISTERED_TS]           DATETIME        NULL,
    [FINAL_TS]                DATETIME        NULL,
    [EFFECTIVE_TS]            DATETIME        NULL,
    [INSTITUTION]             VARCHAR (100)   NULL,
    [PARTICIPANT]             VARCHAR (100)   NULL,
    [CONTRA_INSTITUTION]      VARCHAR (100)   NULL,
    [CONTRA_PARTICIPANT]      VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD] VARCHAR (20)    NULL,
    [EVENT_TRIGGER_CD]        VARCHAR (15)    NULL,
    [EVENT_TRIGGER_DESC]      VARCHAR (100)   NULL,
    [LIMIT_AMOUNT]            DECIMAL (30, 2) NULL,
    [FIXED_TIME]              TIME (7)        NULL,
    [FINALIZED_BY]            VARCHAR (10)    NULL,
    [STATUS_TYPE_CD]          VARCHAR (15)    NULL,
    [LAST_MODIFIED_TS]        DATETIME        NULL,
    PRIMARY KEY CLUSTERED ([LYNX_NET_SEND_LIMIT_ID] ASC, [BATCH_ID] ASC)
);

