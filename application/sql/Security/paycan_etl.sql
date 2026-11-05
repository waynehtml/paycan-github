CREATE ROLE [paycan_etl]
    AUTHORIZATION [dbo];


GO
ALTER ROLE [paycan_etl] ADD MEMBER [lake-cac-npd-adf-devpaycan01];

