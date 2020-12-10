import os
from pathlib import Path

import ee
from dotenv import load_dotenv

# Load dotenv
env_path = Path('..') / '.env'
load_dotenv(dotenv_path=env_path, override=True)

# Init. earth-engine
credentials = ee.ServiceAccountCredentials(os.getenv("SERVICE_ACCOUNT"),
                                           os.getenv("PATH_TO_CREDS"))
ee.Initialize(credentials)


if __name__ == '__main__':
    print('Project Initialized')
