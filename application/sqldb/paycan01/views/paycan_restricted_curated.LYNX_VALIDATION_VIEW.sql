CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_VALIDATION_VIEW]
AS SELECT [BATCH_ID]
      ,[FILE_NAME]
      ,[FILE_PRIMARY_ID]
      ,[BUSINESS_ID]
      ,[FLAG_NAME]
      ,[CREATETIME]
FROM [paycan_restricted_curated].[LYNX_VALIDATION];

