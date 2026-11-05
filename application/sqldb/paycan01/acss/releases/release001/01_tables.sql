IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_standardized].[ACSS_USBE_SETS]') AND type in (N'U'))
DROP TABLE [paycan_restricted_standardized].[ACSS_USBE_SETS];

CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_SETS](
	[acss_usbe_sets_id] [bigint] NOT NULL,
	[batch_id] [int] NOT NULL,
	[system_code] [char](1) NULL,
	[settlement_date] [date] NULL,
	[sending_dc_irn] [varchar](3) NULL,
	[sending_dc_name] [varchar](200) NULL,
	[receiving_dc_irn] [varchar](3) NULL,
	[receiving_dc_name] [varchar](200) NULL,
	[sets_volume] [bigint] NULL,
	[sets_value] [decimal](30, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[acss_usbe_sets_id] ASC,
	[batch_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_standardized].[ACSS_USBE_VOL_VAL]') AND type in (N'U'))
DROP TABLE [paycan_restricted_standardized].[ACSS_USBE_VOL_VAL];

CREATE TABLE [paycan_restricted_standardized].[ACSS_USBE_VOL_VAL](
	[acss_usbe_vol_val_id] [bigint] NOT NULL,
	[batch_id] [int] NOT NULL,
	[system_code] [char](1) NULL,
	[settlement_date] [date] NULL,
	[sending_dc_irn] [varchar](3) NULL,
	[sending_dc_name] [varchar](200) NULL,
	[receiving_dc_irn] [varchar](3) NULL,
	[receiving_dc_name] [varchar](200) NULL,
	[stream_code] [varchar](4) NULL,
	[stream_name] [varchar](200) NULL,
	[volume] [bigint] NULL,
	[value] [decimal](30, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[acss_usbe_vol_val_id] ASC,
	[batch_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];

