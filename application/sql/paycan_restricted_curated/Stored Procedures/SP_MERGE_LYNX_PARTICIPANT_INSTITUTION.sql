CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_PARTICIPANT_INSTITUTION]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_PARTICIPANT_INSTITUTION] AS [Source]           
    ON	[Target].[LYNX_PARTICIPANT_INSTITUTION_ID]	= [Source].[LYNX_PARTICIPANT_INSTITUTION_ID] and
		[Target].[BATCH_ID]							= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]			=	[Source].[VALIDATION_STATUS], 
	[Target].[INSTITUTION_NUMBER]			=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]				=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_DESCRIPTION]		=	[Source].[INSTITUTION_DESCRIPTION], 
	[Target].[INSTITUTION_NAME]				=	[Source].[INSTITUTION_NAME], 
	[Target].[INSTITUTION_STATUS_CD]		=	[Source].[INSTITUTION_STATUS_CD], 
	[Target].[PARTICIPANT_NUMBER]			=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_DESCRIPTION]		=	[Source].[PARTICIPANT_DESCRIPTION], 
	[Target].[PARTICIPANT_NAME]				=	[Source].[PARTICIPANT_NAME], 
	[Target].[PARTICIAPNT_STATUS_CD]		=	[Source].[PARTICIAPNT_STATUS_CD], 
	[Target].[CREDIT_IND]					=	[Source].[CREDIT_IND], 
	[Target].[MAX_CREDIT_AMOUNT]			=	[Source].[MAX_CREDIT_AMOUNT], 
	[Target].[MIN_CREDIT_AMOUNT]			=	[Source].[MIN_CREDIT_AMOUNT],
	[Target].[MONETARY_IND]					=	[Source].[MONETARY_IND], 
	[Target].[ROLE_TYPE_CD]					=	[Source].[ROLE_TYPE_CD], 
	[Target].[BUSINESS_IDENT_CD]			=	[Source].[BUSINESS_IDENT_CD], 
	[Target].[CATEGORY_NO]					=	[Source].[CATEGORY_NO], 
	[Target].[OFFICIAL_ADDR1]				=	[Source].[OFFICIAL_ADDR1], 
	[Target].[OFFICIAL_ADDR2]				=	[Source].[OFFICIAL_ADDR2], 
	[Target].[OFFICIAL_ADDR3]				=	[Source].[OFFICIAL_ADDR3], 
	[Target].[OFFICIAL_ADDR4]				=	[Source].[OFFICIAL_ADDR4], 
	[Target].[OFFICIAL_DESIGNATION]			=	[Source].[OFFICIAL_DESIGNATION], 
	[Target].[OFFICIAL_POSTAL_CD]			=	[Source].[OFFICIAL_POSTAL_CD], 
	[Target].[LIQUIDITY_THRESHOLD_BAL]		=	[Source].[LIQUIDITY_THRESHOLD_BAL], 
	[Target].[EFFECTIVE_TS]					=	[Source].[EFFECTIVE_TS], 
	[Target].[EXPIRE_TS]					=	[Source].[EXPIRE_TS], 
	[Target].[LAST_MODIFIED_TS]				=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]				=	[Source].[ingestion_date],
	[Target].[batch_date]					=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_PARTICIPANT_INSTITUTION_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_DESCRIPTION], 
		[INSTITUTION_NAME], 
		[INSTITUTION_STATUS_CD], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_DESCRIPTION], 
		[PARTICIPANT_NAME], 
		[PARTICIAPNT_STATUS_CD], 
		[CREDIT_IND], 
		[MAX_CREDIT_AMOUNT], 
		[MIN_CREDIT_AMOUNT],
		[MONETARY_IND], 
		[ROLE_TYPE_CD], 
		[BUSINESS_IDENT_CD], 
		[CATEGORY_NO], 
		[OFFICIAL_ADDR1], 
		[OFFICIAL_ADDR2], 
		[OFFICIAL_ADDR3], 
		[OFFICIAL_ADDR4], 
		[OFFICIAL_DESIGNATION], 
		[OFFICIAL_POSTAL_CD], 
		[LIQUIDITY_THRESHOLD_BAL], 
		[EFFECTIVE_TS], 
		[EXPIRE_TS], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_PARTICIPANT_INSTITUTION_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_DESCRIPTION], 
		[Source].[INSTITUTION_NAME], 
		[Source].[INSTITUTION_STATUS_CD], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_DESCRIPTION], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[PARTICIAPNT_STATUS_CD], 
		[Source].[CREDIT_IND], 
		[Source].[MAX_CREDIT_AMOUNT], 
		[Source].[MIN_CREDIT_AMOUNT],
		[Source].[MONETARY_IND], 
		[Source].[ROLE_TYPE_CD], 
		[Source].[BUSINESS_IDENT_CD], 
		[Source].[CATEGORY_NO], 
		[Source].[OFFICIAL_ADDR1], 
		[Source].[OFFICIAL_ADDR2], 
		[Source].[OFFICIAL_ADDR3], 
		[Source].[OFFICIAL_ADDR4], 
		[Source].[OFFICIAL_DESIGNATION], 
		[Source].[OFFICIAL_POSTAL_CD], 
		[Source].[LIQUIDITY_THRESHOLD_BAL], 
		[Source].[EFFECTIVE_TS], 
		[Source].[EXPIRE_TS], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END