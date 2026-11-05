CREATE SCHEMA [paycan_restricted_curated]
    AUTHORIZATION [dbo];


GO
GRANT SELECT
    ON SCHEMA::[paycan_restricted_curated] TO [paycan_read_restricted];

