CREATE SCHEMA [paycan_restricted_derived]
    AUTHORIZATION [dbo];


GO
GRANT SELECT
    ON SCHEMA::[paycan_restricted_derived] TO [paycan_read_restricted];

