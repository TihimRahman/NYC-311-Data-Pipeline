#IMPORTS
import logging
import os
from datetime import datetime

import requests
import json
import time

#                                   CONFIG


BASE_URL    = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
BATCH_SIZE  = 1000
MAX_RECORDS = 10000
MAX_RETRIES = 3
RETRY_DELAY = 5

#S3
S3_BUCKET   = "your-bucket-name"
S3_PREFIX   = "raw/nyc311"






#                                           LOGGING SETUP


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"{LOG_DIR}/nyc311_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger=logging.getLogger(__name__)







#                                               FETCH SINGLE BATCH


def fetch_batch(session: requests.Session, offset: int) -> list:
    params = {
        "$limit": BATCH_SIZE,
        "$offset": 0,
        "$order": "created_date DESC"
    }

    logger.info(f"Fetching batch | offset {offset}")

    response = session.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    records = response.json()
    logger.info(f"Fetched {len(records)} records | offset {offset}")

    return records












#                                                  DATA QUALITY


def check_data_quality():
    pass








#                                                 SAVE BATCH TO S3

def save_to_s3():
    pass







#                                                  ORCHESTRATOR



def run_extraction():
    pass







#                                                   ENTRY POINT



if __name__ == "__main__":
    
    with requests.Session() as session:
        records = fetch_batch(session, offset=0)
        print(records[0])




