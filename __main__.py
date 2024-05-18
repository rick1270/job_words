"""Run all modules to get ids and words"""

import time
import os
import word_scrape as jw

now = int(time.time() * 1000000)

keyword = os.getenv("DATA")
location = os.getenv("LOCATION")  # list
api_key = os.getenv("API_KEY")  # one variable
days = os.getenv("DAYS")  # one variable
FOLDER = "Data"  # one variable
TABLE_NAME = "JOB_WORDS_RAW"  # one variable

for lo in location:
    jw.just_ids(keyword, lo, api_key, days, FOLDER)

df_new = jw.come_together(path=FOLDER, table_name=TABLE_NAME)
df_new.to_pickle(f"{os.getenv("BACKUP_FOLDER")}/Backup{now}")
