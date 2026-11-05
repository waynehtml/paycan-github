CREATE ROLE [paycan_ddl]
    AUTHORIZATION [dbo];


GO
ALTER ROLE [paycan_ddl] ADD MEMBER [lake-dev-paycan-dbservices];


GO
ALTER ROLE [paycan_ddl] ADD MEMBER [lake_paycansupport_rusr_inda];

