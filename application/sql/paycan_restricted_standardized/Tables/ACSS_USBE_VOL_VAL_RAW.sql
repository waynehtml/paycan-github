CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_VOL_VAL_RAW] (
    [system_code]       CHAR (1)      NULL,
    [settlement_date]   VARCHAR (8)   NULL,
    [sending_dc_irn]    VARCHAR (3)   NULL,
    [sending_dc_name]   VARCHAR (200) NULL,
    [receiving_dc_irn]  VARCHAR (3)   NULL,
    [receiving_dc_name] VARCHAR (200) NULL,
    [stream_code]       CHAR (1)      NULL,
    [stream_name]       VARCHAR (200) NULL,
    [volume]            VARCHAR (100) NULL,
    [value]             VARCHAR (100) NULL
);

