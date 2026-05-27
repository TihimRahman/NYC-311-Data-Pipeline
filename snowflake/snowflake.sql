-- ── DATABASE & SCHEMAS ───────────────────────────────────
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
CREATE OR REPLACE STAGE nyc311.bronze.s3_stage
    URL = 's3://ntc311-pipeline/source/'
    STORAGE_INTEGRATION = s3_nyc311_integration;

-- ── BRONZE RAW TABLE ─────────────────────────────────────
-- dbt will manage loads, but keep this table for the raw VARIANT
CREATE TABLE IF NOT EXISTS nyc311.bronze.complaints_raw (
    raw_data            VARIANT         NOT NULL,
    filename            VARCHAR         NOT NULL,
    _dbt_extracted_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _dbt_source_relation VARCHAR         DEFAULT 'S3'
)
COMMENT = 'Raw JSON from NYC 311 API via S3. Loaded by dbt.';



LIST @nyc311.bronze.s3_stage;
USE DATABASE nyc311;
USE SCHEMA bronze;



COPY INTO complaints_raw (raw_data, filename)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME
    FROM @s3_stage  
)
FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE);


SHOW STAGES IN SCHEMA nyc311.bronze;

select*from complaints_raw