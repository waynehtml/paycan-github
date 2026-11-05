CREATE ROLE [paycan_read_restricted]
    AUTHORIZATION [dbo];


GO
ALTER ROLE [paycan_read_restricted] ADD MEMBER [lake_sql_talend_dev];


GO
ALTER ROLE [paycan_read_restricted] ADD MEMBER [lake_dmengineer_rusr_inda];


GO
ALTER ROLE [paycan_read_restricted] ADD MEMBER [lake_sql_paycan_dev];


GO
ALTER ROLE [paycan_read_restricted] ADD MEMBER [lake_paycandata_rusr_inda];


GO
ALTER ROLE [paycan_read_restricted] ADD MEMBER [tabazure_s];

