CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_VOL_VAL] (
    [acss_usbe_vol_val_id] BIGINT          NOT NULL,
    [batch_id]             INT             NOT NULL,
    [system_code]          CHAR (1)        NULL,
    [settlement_date]      DATE            NULL,
    [sending_dc_irn]       VARCHAR (3)     NULL,
    [sending_dc_name]      VARCHAR (200)   NULL,
    [receiving_dc_irn]     VARCHAR (3)     NULL,
    [receiving_dc_name]    VARCHAR (200)   NULL,
    [stream_code]          VARCHAR (4)     NULL,
    [stream_name]          VARCHAR (200)   NULL,
    [volume]               BIGINT          NULL,
    [value]                DECIMAL (30, 2) NULL,
    PRIMARY KEY CLUSTERED ([acss_usbe_vol_val_id] ASC, [batch_id] ASC)
);

