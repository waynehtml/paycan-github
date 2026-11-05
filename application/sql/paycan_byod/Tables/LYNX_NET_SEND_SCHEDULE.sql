CREATE TABLE [paycan_byod].[LYNX_NET_SEND_SCHEDULE] (
    [LYNX_NET_SEND_SCHEDULE_ID] BIGINT          NOT NULL,
    [BATCH_ID]                  INT             NOT NULL,
    [VALIDATION_STATUS]         BIT             NULL,
    [NET_SEND_ID]               BIGINT          NULL,
    [ACTIVE_DATE]               DATE            NULL,
    [DELETE_DATE]               DATE            NULL,
    [CHANGE_TIME]               DATETIME        NULL,
    [INSTITUTION]               VARCHAR (100)   NULL,
    [PARTICIPANT]               VARCHAR (100)   NULL,
    [CONTRA_INSTITUTION]        VARCHAR (100)   NULL,
    [CONTRA_PARTICIPANT]        VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD]   VARCHAR (20)    NULL,
    [DAY_NUMBER]                BIGINT          NULL,
    [FIXED_TIME]                TIME (7)        NULL,
    [EVENT_TRIGGER_CD]          VARCHAR (15)    NULL,
    [EVENT_TRIGGER_DESCRIPTION] VARCHAR (100)   NULL,
    [AMOUNT]                    DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]          DATETIME        NULL,
    PRIMARY KEY CLUSTERED ([LYNX_NET_SEND_SCHEDULE_ID] ASC, [BATCH_ID] ASC)
);

