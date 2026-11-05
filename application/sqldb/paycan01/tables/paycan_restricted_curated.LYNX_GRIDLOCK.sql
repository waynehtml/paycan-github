IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_curated].[LYNX_GRIDLOCK]') AND type in (N'U'))
DROP TABLE [paycan_restricted_curated].[LYNX_GRIDLOCK];

CREATE TABLE [paycan_restricted_curated].[LYNX_GRIDLOCK](
	[LYNX_GRIDLOCK_ID] [bigint] NOT NULL,
	[BATCH_ID] [int] NOT NULL,
	[VALIDATION_STATUS] [bit] NULL,
	[GRIDLOCK_PARTICIPANT_ID] [bigint] NULL,
	[GRIDLOCK_BATCH_ID] [bigint] NULL,
	[CYCLE_DATE] [date] NULL,
	[START_TS] [datetime] NULL,
	[END_TS] [datetime] NULL,
	[TRIGGER] [varchar](20) NULL,
	[GRIDLOCK_INTERVAL] [bigint] NULL,
	[INSTITUTION] [varchar](100) NULL,
	[PARTICIPANT] [varchar](100) NULL,
	[BEGIN_QUEUE_VALUE] [decimal](30, 2) NULL,
	[END_QUEUE_VALUE] [decimal](30, 2) NULL,
	[BEGIN_QUEUE_VOLUME] [bigint] NULL,
	[END_QUEUE_VOLUME] [bigint] NULL,
	[LAST_MODIFIED_TS] [datetime] NULL,
	[STATUS_VALUE] [varchar](256) NULL,
	[STATUS_VOLUME] [varchar](256) NULL,
	[STATUS_COUNT] [int] NULL,
	[STATUS_MATCH] [varchar](256) NULL,
PRIMARY KEY CLUSTERED 
(
	[LYNX_GRIDLOCK_ID] ASC,
	[BATCH_ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];


