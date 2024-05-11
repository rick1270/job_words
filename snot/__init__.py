"""Collection of python snowflake tools"""

import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv, find_dotenv

load_dotenv()

ctx = snowflake.connector.connect(
    account=os.getenv("ACCOUNT"),
    user=os.getenv("SF_USER"),
    password=os.getenv("PASSWORD"),
    role=os.getenv("ROLE"),
    database=os.getenv("DATABASE"),
    warehouse=os.getenv("WAREHOUSE"),
    schema=os.getenv("SCHEMA"),
)


def df_to_table(df=None, table=None) -> dict:
    """Appends df to snowflake table or creates new table"""
    conn = snowflake.connector.connect(
        account=os.getenv("ACCOUNT"),
        user=os.getenv("SF_USER"),
        password=os.getenv("PASSWORD"),
        role=os.getenv("ROLE"),
        database=os.getenv("DATABASE"),
        warehouse=os.getenv("WAREHOUSE"),
        schema=os.getenv("SCHEMA"),
    )
    str_df = df.astype(str)
    success, chunks, rows, snowflake_output = write_pandas(
        conn=conn, df=str_df, table_name=table, auto_create_table=True
    )
    if success is True:
        print(f"Success!  {rows} rows added to {table} in {chunks} chunks")
    else:
        print(f"DID NOT WORK! {table} \n {snowflake_output}")


def current_ids(table_name=None):
    """retrieves existing ids to avoid duplication"""

    with snowflake.connector.connect(
        account=os.getenv("ACCOUNT"),
        user=os.getenv("SF_USER"),
        password=os.getenv("PASSWORD"),
        role=os.getenv("ROLE"),
        database=os.getenv("DATABASE"),
        warehouse=os.getenv("WAREHOUSE"),
        schema=os.getenv("SCHEMA"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f'select "job_id" from {table_name}')
            id_list = []
            for col1 in cur:
                idee = col1[0]
                id_list.append(idee)
    return id_list
