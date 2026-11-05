CREATE   VIEW [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION_RAW]
AS SELECT [INSTITUTION_NUMBER] as idn_institution
	,[INSTITUTION_IRN] as inst_irn
	,[INSTITUTION_NAME] as inst_name
	,[INSTITUTION_DESCRIPTION] as inst_description
	,[INSTITUTION_STATUS_CD] as inst_status_code
	,[PARTICIPANT_NUMBER] as idn_participant
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT_DESCRIPTION] as participant_description
	,[PARTICIAPNT_STATUS_CD] as participant_status_code
	,[CREDIT_IND] as credit_ind
	,[MAX_CREDIT_AMOUNT] as max_credit_amt
	,[MIN_CREDIT_AMOUNT] as min_credit_amt
	,[MONETARY_IND] as monetary_ind
	,[ROLE_TYPE_CD] as role_type
	,[BUSINESS_IDENT_CD] as bic
	,[CATEGORY_NO] as category_no
	,[OFFICIAL_ADDR1] as official_addr1
	,[OFFICIAL_ADDR2] as official_addr2
	,[OFFICIAL_ADDR3] as official_addr3
	,[OFFICIAL_ADDR4] as official_addr4
	,[OFFICIAL_DESIGNATION] as official_designation
	,[OFFICIAL_POSTAL_CD] as official_postal_code
	,[LIQUIDITY_THRESHOLD_BAL] as liquidity_threshold_bal
	,[EFFECTIVE_TS] as effective_time
	,[EXPIRE_TS] as expire_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION];