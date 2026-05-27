from datetime import datetime, timedelta
from textwrap import dedent
from airflow.sdk import dag, task


INGESTION_DIR = "/opt/ingestion"
EXTRACTOR_SCRIPT = "nyc311_extractor.py"



@dag(
    dag_id="nyc311_extraction_pipeline",
    description="Pull NYC 311 complaints from Socrata API → S3 → bronze (via Snowpipe)",
    tags=["nyc311", "extraction", "ingestion", "production"],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "tihim",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
)
def nyc311_extraction_pipeline():
    """Extract from Socrata, write JSON to S3. Snowpipe handles the rest."""

    @task.bash(
        task_id="extract_complaints",
        doc_md=dedent("""
            Run the Python extractor:
              1. Hits the NYC 311 Socrata API
              2. Writes JSON files to S3 (s3://ntc311-pipeline/source/...)
              3. Snowpipe (configured separately in Snowflake) auto-ingests
                 within ~60 seconds.
            
            Credentials sourced from the mounted ~/.aws/credentials.
        """),
    )
    def extract_complaints() -> str:
        return f"cd {INGESTION_DIR} && python {EXTRACTOR_SCRIPT}"

    extract_complaints()


nyc311_extraction_pipeline()