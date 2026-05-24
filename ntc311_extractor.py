#IMPORTS

#CONFIG
BASE_URL    = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
BATCH_SIZE  = 1000
MAX_RECORDS = 10000
MAX_RETRIES = 3
RETRY_DELAY = 5

#S3
S3_BUCKET   = "your-bucket-name"
S3_PREFIX   = "raw/nyc311"

#LOGGING SETUP


#FETCH SINGLE BATCH
def fetch_batch():
    pass

#DATA QUALITY
def check_data_quality():
    pass

#SAVE BATCH TO S3
def save_to_s3():
    pass

#ORCHESTRATOR
def run_extraction():
    pass


#MAIN

if __name__ == "__main__":
    pass




