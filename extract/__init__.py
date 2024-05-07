"""Filter and extract job data based on ids"""

import os
import glob
import json
from datetime import datetime as dt
import store_retrieve
import scrape
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
scraper_api = os.getenv("API_KEY")

def get_df_ids(
    file="Data_Words/Data/job_words_indeed.pkl",
):
    """Retrieves main df from csv and returns the Job Id column as a list
    Used later to filter out ids that have already been processed"""
    try:
        df = pd.read_pickle(file)
        df_ids = list(df["job_id"])
        return df_ids
    except FileNotFoundError:
        print(f"no pkl found in {file}")
        return ["x"]

def get_job_dict(id_tuple):
    """Makes api call and converts html response to soup,
    extracts specific json, converts to dict return"""
    keyword = id_tuple[0]
    location = id_tuple[1]
    id_number = id_tuple[2]
    url = id_tuple[3]
    r = scrape.scrape(3, scraper_api, url).text
    try:
        # create soup from r
        soup = BeautifulSoup(r, "html.parser")
        # find and extract json from soup
        j_soup = soup.find("script", type="application/ld+json").text
        # convert json to dict
        d_soup = json.loads(j_soup)
        # Append data from tuple
        d_soup.update(
            {
                "jobId": id_number,
                "jobKeyword": keyword,
                "jobSearchLocation": location,
            }
        )
        # print(f'{dt.now()}get_job_dict try')
        return d_soup
    except AttributeError:
        # print(f'{dt.now()}get_job_dict ex')
        return {"jobId": id, "jobKeyword": keyword, "jobSearchLocation": location}

# Ids ready
def ids_warming(path):
    """Extract ids from json files.  Ouput list of tuples with keyword, location, id, URL"""
    id_tuple_list = []
    # get ids that have already run
    df_ids = get_df_ids()
    # list of id file names
    ids = glob.glob(f"{path}/indeedIds_*.json")
    # loop through file names extract kw and location
    for d in ids:
        id_path = d.split("/")[-1]
        id_name = id_path.split("_")
        kw = id_name[1]
        loc = id_name[2].split(".")[0]
        # open each id file
        with open(id, "r", encoding="utf-8") as f:
            j_ids = json.load(f)[id]
            # if id already in df skip
            for j in j_ids:
                if j in df_ids:
                    pass
                elif kw in ("test", "write"):
                    pass
                # not in df... create tup using (kw,loc,id) and append to list
                else:
                    url = f"https://www.indeed.com/m/viewjob?jk={j}"
                    id_tup = (kw, loc, j, url)
                    id_tuple_list.append(id_tup)
    # return set of all unique unprocessed tuples
    tuple_dd_list = store_retrieve.dd(id_tuple_list)
    print(f"{len(tuple_dd_list)} unique job ids will be processed")
    return tuple_dd_list

# This func does not work as stand alone
def li_li_li_list(description):
    """Takes description dict and returns li values as list"""
    # Resoup dict
    d_soup = BeautifulSoup(description, "html.parser")
    # get list of li tag text
    lis = d_soup.find_all("li")
    tag_list = []
    for l in lis:
        # get just li text and put back in list
        bullet = l.text
        tag_list.append(bullet)
    # print(f'{dt.now()}li_li_li_list')
    return tag_list
