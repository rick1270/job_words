"""Run all modules to get ids and words"""

import time
import os
import snowflake.connector
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
SF_SKILL_WORD_TABLE = os.getenv("SF_SKILL_WORD_TABLE")
SF_TECHNOLOGY_WORD_TABLE = os.getenv("SF_TECHNOLOGY_WORD_TABLE")
SF_MATCH_COLUMN_SKILLS = os.getenv("SF_MATCH_COLUMN_SKILLS")
SF_MATCH_COLUMN_TECHNOLOGY = os.getenv("SF_MATCH_COLUMN_TECHNOLOGY")
DATA_FOLDER_PATH = os.getenv("DATA_FOLDER_PATH")
BACKUP_FOLDER = os.getenv("BACKUP_FOLDER")


def x_obscure(string):
    """Replaces each character in a string with x"""
    letters = []
    for _ in string:
        letters.append("x")
    xx = "".join(letters)
    return xx


def get_snowflake_column(
    sf_column_name,
    sf_table_name,
    sf_account,
    sf_user,
    sf_password,
    sf_database,
    sf_warehouse,
    sf_schema,
):
    """gets one column from snowflake table"""
    sql = f"select {sf_column_name} from {sf_table_name}"
    with snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            c_list = []
            for col1 in cur:
                c = col1[0]
                c_list.append(c)
    return c_list


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
    sf_skill_word_table {SF_SKILL_WORD_TABLE}
    sf_technology_word_table {SF_TECHNOLOGY_WORD_TABLE}
    sf_match_column_skills: {SF_MATCH_COLUMN_SKILLS}
    sf_match_column_technology: {SF_MATCH_COLUMN_TECHNOLOGY}
    data_folder_path: {DATA_FOLDER_PATH}
    backup_folder_path: {BACKUP_FOLDER}"""
)

skill_word_list = get_snowflake_column(
    SF_MATCH_COLUMN_SKILLS,
    SF_SKILL_WORD_TABLE,
    ACCOUNT,
    SF_USER,
    PASSWORD,
    DATABASE,
    WAREHOUSE,
    SCHEMA,
)


technology_word_list = get_snowflake_column(
    SF_MATCH_COLUMN_TECHNOLOGY,
    SF_TECHNOLOGY_WORD_TABLE,
    ACCOUNT,
    SF_USER,
    PASSWORD,
    DATABASE,
    WAREHOUSE,
    SCHEMA,
)

jw.just_ids(KEYWORD, LOCATION, API_KEY, DAYS, FOLDER)

df_new = jw.come_together(
    FOLDER,
    TABLE_NAME,
    API_KEY,
    ACCOUNT,
    SF_USER,
    PASSWORD,
    DATABASE,
    WAREHOUSE,
    SCHEMA,
    skill_word_list,
    SF_MATCH_COLUMN_SKILLS,
    technology_word_list,
    SF_MATCH_COLUMN_TECHNOLOGY,
)
# df_new.to_pickle(f"{BACKUP_FOLDER}/Backup{now}")
