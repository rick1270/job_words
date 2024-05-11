"""Run all modules to get ids and words"""

import time
import json
import os
import ids
import words
import snot

now = int(time.time() * 1000000)
FILE_NAME = "variables.json"
with open(FILE_NAME, "r", encoding="utf-8") as f:
    variable = json.load(f)

keyword = variable["variables"]["keyword"]  # one variable
location = variable["variables"]["location"]  # list
api_key = os.getenv("API_KEY")  # one variable
days = variable["variables"]["days"]  # one variable
folder = "Data"  # one variable
TABLE_NAME = "PC_DBT_DB.RAW.JOB_WORDS_RAW"  # one variable

for l in location:
    ids.just_ids(keyword, l, api_key, days, folder)

df_new = words.come_together(path=folder, table_name=TABLE_NAME)
df_new.to_pickle(f"Backups/Backup{now}")
print(df_new)
snot.df_to_table(df_new, TABLE_NAME)
