CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_LOAN]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_LOAN] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_LOAN] AS [Source]           
    ON	[Target].[LYNX_LOAN_ID]	= [Source].[LYNX_LOAN_ID] and
		[Target].[BATCH_ID]		= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]		=	[Source].[VALIDATION_STATUS], 
	[Target].[LOAN_ENTRY_ID]			=	[Source].[LOAN_ENTRY_ID], 
	[Target].[CYCLE_DATE]				=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]		=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]			=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]			=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]		=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]			=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]			=	[Source].[PARTICIPANT_NAME], 
	[Target].[ACCOUNT_ID]				=	[Source].[ACCOUNT_ID], 
	[Target].[SYSTEM_REFERENCE_CD]		=	[Source].[SYSTEM_REFERENCE_CD], 
	[Target].[INSTITUTION]				=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]				=	[Source].[PARTICIPANT], 
	[Target].[ACCOUNT_NAME]				=	[Source].[ACCOUNT_NAME], 
	[Target].[ENTRY_TYPE]				=	[Source].[ENTRY_TYPE], 
	[Target].[AMOUNT]					=	[Source].[AMOUNT], 
	[Target].[ENTRY_TS]					=	[Source].[ENTRY_TS], 
	[Target].[ACCOUNT_BALANCE]			=	[Source].[ACCOUNT_BALANCE],
	[Target].[LAST_MODIFIED_TS]			=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]			=	[Source].[ingestion_date],
	[Target].[batch_date]				=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_LOAN_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[LOAN_ENTRY_ID], 
		[CYCLE_DATE], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[ACCOUNT_ID], 
		[SYSTEM_REFERENCE_CD], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[ACCOUNT_NAME], 
		[ENTRY_TYPE], 
		[AMOUNT], 
		[ENTRY_TS], 
		[ACCOUNT_BALANCE],
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_LOAN_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[LOAN_ENTRY_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[ACCOUNT_ID], 
		[Source].[SYSTEM_REFERENCE_CD], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[ACCOUNT_NAME], 
		[Source].[ENTRY_TYPE], 
		[Source].[AMOUNT], 
		[Source].[ENTRY_TS], 
		[Source].[ACCOUNT_BALANCE],
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END