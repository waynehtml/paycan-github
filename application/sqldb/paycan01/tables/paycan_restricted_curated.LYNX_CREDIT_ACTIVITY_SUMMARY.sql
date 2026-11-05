IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY]') AND type in (N'U'))
DROP TABLE [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY];

CREATE TABLE [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY](
	[BATCH_ID] [int] NULL,
	[CREDIT_ACTIVITY_ID] [bigint] NULL,
	[CYCLE_DATE] [date] NULL,
	[INSTITUTION] [varchar](100) NULL,
	[PARTICIPANT] [varchar](100) NULL,
	[TOTAL_AMOUNT] [decimal](22, 2) NULL,
	[EARMARKED_AMOUNT] [decimal](22, 2) NULL,
	[EARMARKED_LOM] [varchar](100) NULL,
	[REGISTERED_TIME] [datetime] NULL,
	[PROCESSED_TIME] [datetime] NULL,
	[REGISTERED] [tinyint] NULL,
	[PROCESSED] [tinyint] NULL,
	[TOTAL_DURATION] [bigint] NULL
) ON [PRIMARY];

