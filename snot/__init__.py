"""Collection of python snowflake tools"""


import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def df_to_table(df, table_name, connection_parameters) ->dict:
    """Appends df to snowflake table or creates new table"""
    ctx = snowflake.connector.connect(
    user=connection_parameters['user'],
    password=connection_parameters['password'],
    account=connection_parameters['account'],
    warehouse=connection_parameters['warehouse'],
    database=connection_parameters['database'],
    schema=connection_parameters['schema'],
    )
    ctx.cursor()
    table = table_name
    str_df = df.astype(str)
    success, chunks, rows, snowflake_output = write_pandas(
        ctx, str_df, table, auto_create_table=True)
    if success is True:
        print(f'Success!  {rows} rows added to {table} in {chunks} chunks')
    else:
        print(f'DID NOT WORK! {table} \n {snowflake_output}')
