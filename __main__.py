"""Run all modules to get ids and words"""

import time
import os
import word_scrape as jw

now = int(time.time() * 1000000)

KEYWORD = os.getenv("KEYWORD")
LOCATION = os.getenv("LOCATION")  # list
API_KEY = os.getenv("API_KEY")  # one variable
DAYS = os.getenv("DAYS")  # one variable
FOLDER = os.getenv("FOLDER")  # one variable
TABLE_NAME = os.getenv("TABLE_NAME")  # one variable
SF_USER = os.getenv("SF_USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")
ROLE = os.getenv("ROLE")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
MATCH_WORDS = os.getenv("MATCH_WORDS")
MATCH_COLUMN_SKILLS = os.getenv("MATCH_COLUMN_SKILLS")
MATCH_COLUMN_TECHNOLOGY = os.getenv("MATCH_COLUMN_TECHNOLOGY")
DATA_FOLDER_PATH = os.getenv("DATA_FOLDER_PATH")
BACKUP_FOLDER = os.getenv("BACKUP_FOLDER")


def x_obscure(string):
    """Replaces each character in a string with x"""
    letters = []
    for _ in string:
        letters.append("x")
    xx = "".join(letters)
    return xx


print(
    f"""Using Evironmental Variables
    keyword: {KEYWORD}
    location: {LOCATION}
    api_key: {x_obscure(API_KEY)}
    days: {DAYS}
    folder: {FOLDER}
    table_name: {TABLE_NAME}
    sf_user: {SF_USER}
    sf_password: {x_obscure(PASSWORD)}
    sf_account: {ACCOUNT}
    sf_role: {ROLE}
    sf_warehouse: {WAREHOUSE}
    sf_database: {DATABASE}
    sf_schema: {SCHEMA}
    match_word_location: {MATCH_WORDS}
    match_column_skills_name: {MATCH_COLUMN_SKILLS}
    match_column_technology_name: {MATCH_COLUMN_TECHNOLOGY}
    data_folder_path: {DATA_FOLDER_PATH}
    backup_folder_path: {BACKUP_FOLDER}"""
)

for lo in " ".split(LOCATION):
    jw.just_ids(KEYWORD, lo, API_KEY, DAYS, FOLDER)

df_new = jw.come_together(
    FOLDER, TABLE_NAME, API_KEY, ACCOUNT, SF_USER, PASSWORD, DATABASE, WAREHOUSE, SCHEMA
)
df_new.to_pickle(f"{BACKUP_FOLDER}/Backup{now}")
