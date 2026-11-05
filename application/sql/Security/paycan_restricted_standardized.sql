CREATE SCHEMA [paycan_restricted_standardized]
    AUTHORIZATION [dbo];


GO
GRANT SELECT
    ON SCHEMA::[paycan_restricted_standardized] TO [paycan_read_restricted];

