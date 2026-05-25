CREATE DATABASE IF NOT EXISTS nyc311;

CREATE SCHEMA IF NOT EXISTS nyc311.bronze;
CREATE SCHEMA IF NOT EXISTS nyc311.silver;
CREATE SCHEMA IF NOT EXISTS nyc311.gold;

-- ── WAREHOUSE ────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS nyc311_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ── S3 INTEGRATION ───────────────────────────────────────
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_nyc311_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://ntc311-pipeline/')
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::425750453753:role/pipeline4-snowflake-role';

-- ── EXTERNAL STAGE ───────────────────────────────────────
USE DATABASE nyc311;
USE SCHEMA bronze;

CREATE OR REPLACE STAGE nyc311_s3_stage
    URL = 's3://ntc311-pipeline/source/'
    STORAGE_INTEGRATION = s3_nyc311_integration
    FILE_FORMAT = (TYPE = 'JSON');

-- ── BRONZE TABLE ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nyc311.bronze.complaints_json (
    raw_data    VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    filename    VARCHAR
);

-- ── LOAD FROM S3 ─────────────────────────────────────────
COPY INTO nyc311.bronze.complaints_json (raw_data, filename)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME
    FROM @nyc311_s3_stage
)
FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE);

-- ── VERIFY ───────────────────────────────────────────────
SELECT 
    raw_data:unique_key::STRING     as unique_key,
    raw_data:borough::STRING        as borough,
    raw_data:complaint_type::STRING as complaint_type,
    raw_data:created_date::TIMESTAMP as created_date
FROM nyc311.bronze.complaints_json
LIMIT 5;