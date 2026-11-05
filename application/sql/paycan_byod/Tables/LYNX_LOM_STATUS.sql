CREATE TABLE [paycan_byod].[LYNX_LOM_STATUS] (
    [LYNX_LOM_STATUS_ID]      BIGINT        NOT NULL,
    [BATCH_ID]                INT           NOT NULL,
    [VALIDATION_STATUS]       BIT           NULL,
    [CYCLE_DATE]              DATE          NULL,
    [INSTITUTION]             VARCHAR (100) NULL,
    [PARTICIPANT]             VARCHAR (100) NULL,
    [SETTLEMENT_MECHANISM_CD] VARCHAR (20)  NULL,
    [STATUS_CD]               VARCHAR (10)  NULL,
    [EFFECTIVE_TS]            DATETIME      NULL,
    [LAST_MODIFIED_TS]        DATETIME      NULL,
    PRIMARY KEY CLUSTERED ([LYNX_LOM_STATUS_ID] ASC, [BATCH_ID] ASC)
);

