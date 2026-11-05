CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_SETS] (
    [acss_usbe_sets_id] BIGINT          NOT NULL,
    [batch_id]          INT             NOT NULL,
    [system_code]       CHAR (1)        NULL,
    [settlement_date]   DATE            NULL,
    [sending_dc_irn]    VARCHAR (3)     NULL,
    [sending_dc_name]   VARCHAR (200)   NULL,
    [receiving_dc_irn]  VARCHAR (3)     NULL,
    [receiving_dc_name] VARCHAR (200)   NULL,
    [sets_volume]       BIGINT          NULL,
    [sets_value]        DECIMAL (30, 2) NULL,
    PRIMARY KEY CLUSTERED ([acss_usbe_sets_id] ASC, [batch_id] ASC)
);

