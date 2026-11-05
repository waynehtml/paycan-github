IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY]') AND type in (N'U'))
DROP TABLE [paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY];

CREATE TABLE [paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY](
	[BATCH_ID] [int] NULL,
	[TRANSFER_ID] [bigint] NULL,
	[PAYER_REFERENCE] [varchar](100) NULL,
	[CYCLE_DATE] [date] NULL,
	[INSTITUTION] [varchar](100) NULL,
	[PARTICIPANT] [varchar](100) NULL,
	[FROM_ACCOUNT_BBAN] [varchar](100) NULL,
	[FROM_ACCOUNT_NAME] [varchar](100) NULL,
	[FROM_SETTLE_MECH_CD] [varchar](100) NULL,
	[TO_ACCOUNT_BBAN] [varchar](100) NULL,
	[TO_ACCOUNT_NAME] [varchar](100) NULL,
	[TO_SETTLE_MECH_CD] [varchar](100) NULL,
	[TRANSACTION_TYPE] [varchar](100) NULL,
	[AMOUNT] [decimal](22, 2) NULL,
	[REGISTERED_TIME] [datetime] NULL,
	[SETTLED_TIME] [datetime] NULL,
	[PROCESSED_TIME] [datetime] NULL,
	[DIARISED_TIME] [datetime] NULL,
	[REGISTERED] [tinyint] NULL,
	[SETTLED] [tinyint] NULL,
	[PROCESSED] [tinyint] NULL,
	[DIARISED] [tinyint] NULL,
	[TOTAL_DURATION] [bigint] NULL
) ON [PRIMARY];

