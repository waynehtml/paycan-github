IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[paycan_restricted_curated].[LYNX_LOAN]') AND type in (N'U'))
DROP TABLE [paycan_restricted_curated].[LYNX_LOAN];

CREATE TABLE [paycan_restricted_curated].[LYNX_LOAN](
	[LYNX_LOAN_ID] [bigint] NOT NULL,
	[BATCH_ID] [int] NOT NULL,
	[VALIDATION_STATUS] [bit] NULL,
	[LOAN_ENTRY_ID] [bigint] NULL,
	[CYCLE_DATE] [date] NULL,
	[INSTITUTION] [varchar](100) NULL,
	[PARTICIPANT] [varchar](100) NULL,
	[ACCOUNT_NAME] [varchar](10) NULL,
	[ENTRY_TYPE] [varchar](25) NULL,
	[AMOUNT] [decimal](22, 2) NULL,
	[ENTRY_TS] [datetime] NULL,
	[ACCOUNT_BALANCE] [decimal](30, 2) NULL,
	[LAST_MODIFIED_TS] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[LYNX_LOAN_ID] ASC,
	[BATCH_ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];


