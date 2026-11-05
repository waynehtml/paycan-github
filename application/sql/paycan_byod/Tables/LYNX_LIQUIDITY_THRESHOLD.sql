CREATE TABLE [paycan_byod].[LYNX_LIQUIDITY_THRESHOLD] (
    [LYNX_LIQUIDITY_THRESHOLD_ID] BIGINT          NOT NULL,
    [BATCH_ID]                    INT             NOT NULL,
    [VALIDATION_STATUS]           BIT             NULL,
    [CREATE_DATE]                 DATE            NULL,
    [ACTIVE_DATE]                 DATE            NULL,
    [DELETE_DATE]                 DATE            NULL,
    [CHANGE_TS]                   DATETIME        NULL,
    [INSTITUTION]                 VARCHAR (100)   NULL,
    [PARTICIPANT]                 VARCHAR (100)   NULL,
    [SETTLEMENT_MECHANISM_CD]     VARCHAR (20)    NULL,
    [THRESHOLD_AMOUNT]            DECIMAL (30, 2) NULL,
    [LAST_MODIFIED_TS]            DATETIME        NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LIQUIDITY_THRESHOLD_ID] ASC, [BATCH_ID] ASC)
);

