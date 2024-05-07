"""Run all modules to get ids and words"""

import json
import catch_dict
import extract
import ids
import names
import scrape
import snot
import spac
import store_retrieve
import words

FILE_NAME = 'job_words/variables.json'
with open(FILE_NAME, "r", encoding="utf-8") as f:
    variable = json.load(f)

keyword = variable['keyword'] #one variable
location = variable['location'] #list
api_key = variable['api_key'] #one variable
days = variable['days'] #one variable
folder = variable['folder_path']


for l in location:
    ids.just_ids(keyword,l,api_key,days, folder)

words.come_together()
