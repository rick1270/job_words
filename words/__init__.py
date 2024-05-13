"""Combines and executes word functions to create final output"""

import traceback
import time
import pandas as pd
import ids
import extract
import catch_dict
import snot

now = int(time.time() * 1000000)


def come_together(path, table_name):
    """runs scraped ids and returns list df"""
    dict_list = []
    tup_list = extract.ids_warming(
        path, table_name
    )  # returns list of id tuples  ->list
    er_count = 0
    try:
        for tup in tup_list:
            try:
                job_dict = extract.get_job_dict(
                    tup
                )  # takes one id tuple and outputs dict with data -> dict
                new_job_dict = catch_dict.check_and_extract(
                    job_dict
                )  #  (description_list:[]) -> dict
                # should only run ids not already in table
                dict_list.append(new_job_dict)
                print(f"{len(dict_list)} jobs out of {len(tup_list)} processed")
            except TimeoutError:
                traceback.print_exc()
                er_count += 1
                er_retry = er_count * 60
                print(f"Error has occured.  Will retry in {er_retry}")
        df_new = pd.DataFrame(dict_list)
        snot.df_to_table(df_new, table_name)
        return df_new
    except KeyboardInterrupt:
        df_new = pd.DataFrame(dict_list)
        snot.df_to_table(df_new, table_name)
        return df_new
