CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_SETS_RAW] (
    [system_code]       CHAR (1)      NULL,
    [settlement_date]   VARCHAR (8)   NULL,
    [sending_dc_irn]    VARCHAR (3)   NULL,
    [sending_dc_name]   VARCHAR (200) NULL,
    [receiving_dc_irn]  VARCHAR (3)   NULL,
    [receiving_dc_name] VARCHAR (200) NULL,
    [sets_volume]       VARCHAR (100) NULL,
    [sets_value]        VARCHAR (100) NULL
);

