
CREATE   VIEW [paycan_restricted_curated].[LYNX_LOM_STATUS_VIEW]
AS SELECT [LYNX_LOM_STATUS_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[STATUS_CD]
      ,[EFFECTIVE_TS]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_LOM_STATUS];