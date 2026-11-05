IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY]') AND type in (N'U'))
DROP TABLE [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY];

CREATE TABLE [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY](
	[BATCH_ID] [int] NULL,
	[PAYMENT_ID] [bigint] NULL,
	[SYSTEM_REFERENCE] [varchar](100) NULL,
	[CYCLE_DATE] [date] NULL,
	[PAYER_INSTITUTION] [varchar](100) NULL,
	[PAYER_PARTICIPANT] [varchar](100) NULL,
	[SETTLEMENT_MECHANISM_CD] [varchar](100) NULL,
	[RECEIVER_INSTITUTION] [varchar](100) NULL,
	[RECEIVER_PARTICIPANT] [varchar](100) NULL,
	[SWIFT_MSG_TYPE_CD] [varchar](100) NULL,
	[TRANSACTION_TYPE] [varchar](100) NULL,
	[AMOUNT] [decimal](22, 2) NULL,
	[GRIDLOCK_SETTLED_ID] [bigint] NULL,
	[PRIORITY] [bigint] NULL,
	[REGISTERED_TIME] [datetime] NULL,
	[SETTLED_TIME] [datetime] NULL,
	[PROCESSED_TIME] [datetime] NULL,
	[QUEUED_TIME] [datetimeoffset](7) NULL,
	[REGISTERED] [tinyint] NULL,
	[SETTLED] [tinyint] NULL,
	[PROCESSED] [tinyint] NULL,
	[QUEUED] [tinyint] NULL,
	[TOTAL_DURATION] [bigint] NULL
) ON [PRIMARY];

