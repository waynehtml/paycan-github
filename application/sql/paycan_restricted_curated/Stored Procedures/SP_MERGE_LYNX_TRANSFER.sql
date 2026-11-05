CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_TRANSFER]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_TRANSFER] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_TRANSFER] AS [Source]           
    ON	[Target].[LYNX_TRANSFER_ID]	= [Source].[LYNX_TRANSFER_ID] and
		[Target].[BATCH_ID]			= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]			=	[Source].[VALIDATION_STATUS],
	[Target].[TRANSFER_ID]					=	[Source].[TRANSFER_ID], 
	[Target].[SYSTEM_REFERENCE]				=	[Source].[SYSTEM_REFERENCE], 
	[Target].[PAYER_REFERENCE]				=	[Source].[PAYER_REFERENCE], 
	[Target].[CYCLE_DATE]					=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]			=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]				=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]				=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]			=	[Source].[PARTICIPANT_NUMBER],
	[Target].[PARTICIPANT_BIC]				=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]				=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]					=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]					=	[Source].[PARTICIPANT], 
	[Target].[FROM_ACCOUNT_ID]				=	[Source].[FROM_ACCOUNT_ID], 
	[Target].[FROM_ACCOUNT_BBAN]			=	[Source].[FROM_ACCOUNT_BBAN], 
	[Target].[FROM_ACCOUNT_NAME]			=	[Source].[FROM_ACCOUNT_NAME], 
	[Target].[FROM_SETTLE_MECH_CD]			=	[Source].[FROM_SETTLE_MECH_CD],
	[Target].[TO_ACCOUNT_ID]				=	[Source].[TO_ACCOUNT_ID], 
	[Target].[TO_ACCOUNT_BBAN]				=	[Source].[TO_ACCOUNT_BBAN], 
	[Target].[TO_ACCOUNT_NAME]				=	[Source].[TO_ACCOUNT_NAME], 
	[Target].[TO_SETTLE_MECH_CD]			=	[Source].[TO_SETTLE_MECH_CD],
	[Target].[TRANSACTION_TYPE]				=	[Source].[TRANSACTION_TYPE], 
	[Target].[AMOUNT]						=	[Source].[AMOUNT], 
	[Target].[LAST_MODIFIED_TS]				=	[Source].[LAST_MODIFIED_TS], 
	[Target].[STATUS_TYPE_CD]				=	[Source].[STATUS_TYPE_CD], 
	[Target].[STATUS_CHANGE_TS]				=	[Source].[STATUS_CHANGE_TS], 
	[Target].[STATUS_REASON]				=	[Source].[STATUS_REASON], 
	[Target].[STATUS_MILLISECOND_DURATION]	=	[Source].[STATUS_MILLISECOND_DURATION], 
	[Target].[ingestion_date]				=	[Source].[ingestion_date],
	[Target].[batch_date]					=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(		
		[LYNX_TRANSFER_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS],
		[TRANSFER_ID], 
		[SYSTEM_REFERENCE], 
		[PAYER_REFERENCE], 
		[CYCLE_DATE], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER],
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[FROM_ACCOUNT_ID], 
		[FROM_ACCOUNT_BBAN], 
		[FROM_ACCOUNT_NAME], 
		[FROM_SETTLE_MECH_CD],
		[TO_ACCOUNT_ID], 
		[TO_ACCOUNT_BBAN], 
		[TO_ACCOUNT_NAME], 
		[TO_SETTLE_MECH_CD],
		[TRANSACTION_TYPE], 
		[AMOUNT], 
		[LAST_MODIFIED_TS], 
		[STATUS_TYPE_CD], 
		[STATUS_CHANGE_TS], 
		[STATUS_REASON], 
		[STATUS_MILLISECOND_DURATION], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_TRANSFER_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS],
		[Source].[TRANSFER_ID], 
		[Source].[SYSTEM_REFERENCE], 
		[Source].[PAYER_REFERENCE], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER],
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[FROM_ACCOUNT_ID], 
		[Source].[FROM_ACCOUNT_BBAN], 
		[Source].[FROM_ACCOUNT_NAME], 
		[Source].[FROM_SETTLE_MECH_CD],
		[Source].[TO_ACCOUNT_ID], 
		[Source].[TO_ACCOUNT_BBAN], 
		[Source].[TO_ACCOUNT_NAME], 
		[Source].[TO_SETTLE_MECH_CD],
		[Source].[TRANSACTION_TYPE], 
		[Source].[AMOUNT], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[STATUS_TYPE_CD], 
		[Source].[STATUS_CHANGE_TS], 
		[Source].[STATUS_REASON], 
		[Source].[STATUS_MILLISECOND_DURATION], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END