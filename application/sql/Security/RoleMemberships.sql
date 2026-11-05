ALTER ROLE [db_owner] ADD MEMBER [runspdevpaycan];


GO
ALTER ROLE [db_owner] ADD MEMBER [runspact];


GO
ALTER ROLE [db_owner] ADD MEMBER [lake_aeengsupport_rusr_inda];


GO
ALTER ROLE [db_owner] ADD MEMBER [paycan_etl];


GO
ALTER ROLE [db_ddladmin] ADD MEMBER [paycan_etl];


GO
ALTER ROLE [db_ddladmin] ADD MEMBER [paycan_ddl];


GO
ALTER ROLE [db_datawriter] ADD MEMBER [paycan_etl];


GO
ALTER ROLE [db_datawriter] ADD MEMBER [paycan_ddl];


GO
ALTER ROLE [db_datareader] ADD MEMBER [paycan_etl];


GO
ALTER ROLE [db_datareader] ADD MEMBER [paycan_ddl];

