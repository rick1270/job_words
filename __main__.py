"""Run all modules to get ids and words"""

import time
import json
import os
import ids
import words

now = int(time.time() * 1000000)
FILE_NAME = "variables.json"
with open(FILE_NAME, "r", encoding="utf-8") as f:
    variable = json.load(f)

keyword = variable["variables"]["keyword"]  # one variable
location = variable["variables"]["location"]  # list
api_key = os.getenv("API_KEY")  # one variable
days = variable["variables"]["days"]  # one variable
FOLDER = "Data"  # one variable
TABLE_NAME = "JOB_WORDS_RAW"  # one variable

for l in location:
    ids.just_ids(keyword, l, api_key, days, FOLDER)

df_new = words.come_together(path=FOLDER, table_name=TABLE_NAME)
df_new.to_pickle(f"Backups/Backup{now}")
print(df_new)
