"""Dynamic scraping tool for indeed"""

import json
from datetime import datetime as dt
import os
import glob
from itertools import chain
import time
import traceback
import requests
import urllib3
from bs4 import BeautifulSoup
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
import spacy
from spacy.matcher import Matcher


nlp = spacy.load("en_core_web_lg")
stopwords = list(nlp.Defaults.stop_words)
now = int(time.time() * 1000000)

# names


def url_next(keyw, locat, da, last_id=None, page_count=0):
    """Create start url for ids if lastId = None.  Otherwise uses id from previous request"""
    url = f"https://www.indeed.com/jobs?q={keyw}&l={locat}&radius=15&sort=date&fromage={da}"
    if page_count == 0:
        return url
    return f"{url}&start={page_count * 10}&vjk={last_id}"


def file_name_ids(data_folder_path, keyw, locat):
    """creates json file name for ids using keyword and location"""
    file_name = f"{data_folder_path}/indeedIds_{keyw}_{locat}.json"
    return file_name


# store_retrieve


def dd(any_list):
    """Converts list to set then back to list for the purpose of removing duplicates"""
    list_to_set = set(any_list)
    return list(list_to_set)


def j_load(file_name):
    """Retreives existing ids or creates new file.  Returns tuple with id list and count.
    Dedupes as precaution."""
    try:
        with open(file_name, "r", encoding="utf-8") as f:
            ja = json.load(f)
        og_ids = ja[file_name]
        og_list_dd = dd(og_ids)
        print(f"Starting with {len(og_list_dd)} existing ids for {file_name}")
        return (len(og_list_dd), og_list_dd)
    except (FileNotFoundError, TypeError, KeyError, ValueError):
        print(f"No file found using {file_name} starting fresh.")
        return (0, [])


def j_dump(file_name, ids):
    """saves ids as json"""
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump({file_name: dd(ids)}, f)
    print(f"{len(ids)} ids saved to {file_name}")


# scrape
# Uses proxies and redirects to automatically scrape id html.  Requires key.
def scrape(retries, api, url):
    """request and return html from job listing page with retries
    and results printed.  Single thread.  Returned html is used to
    make next url.  Returns r"""
    tries = 0
    while tries <= retries:
        payload = {"api_key": api, "url": url}
        print(f"Calling: {url}")
        r = requests.get("http://api.scraperapi.com", params=payload, timeout=30)
        try:
            if r.headers["sa-statusCode"] == "200":
                print(
                    f"{r.headers['Date']}   {r.headers['sa-final-url']} \
                      status: {r.headers['sa-statusCode']}"
                )
            return r
        except KeyError:
            print(f"{r.headers}")
            time.sleep(300 * tries)
            tries += 1
        print(rf"Try {tries} of {retries} n\ {r.headers}")
    return print(f"{url} has problems")


def calling(keyw, locat, da, api, last_id, page_count):
    """Bundles methods and returns id html."""
    id_url = url_next(keyw, locat, da, last_id, page_count)
    r = scrape(3, api, id_url)
    return r


# ids
def get_ids(r):
    """convert search page response to list of ids."""
    soup = BeautifulSoup(r.text, "html.parser")
    td_soup = soup.find_all("h2")
    jk_list = []
    for soup in td_soup:
        aa = soup.a
        try:
            jk = aa.attrs["data-jk"]
            jk_list.append(jk)
        except AttributeError:
            pass
    return jk_list


def get_new_ids(r, file_name, old_ids):
    # takes allIds and pageCount from previous loop if available
    """Returns a uniques set of ids consisting of old ids and this loop."""
    # loads saved file if first loop

    if len(old_ids) == 0:
        existing_ids_load = j_load(file_name)
        existing_id_count = existing_ids_load[0]
        existing_ids = existing_ids_load[1]
        existing_id_dd = dd(existing_ids)
        print(f"starting id count is {existing_ids_load[0]}")
    else:
        existing_id_dd = dd(old_ids)
        existing_id_count = len(existing_id_dd)
    try:
        loop_id_list = get_ids(r)
        all_unique = dd(existing_id_dd + loop_id_list)
    except (NameError, TypeError, AttributeError):
        all_unique = []
    all_unique_count = len(all_unique)
    loop_unique_count = all_unique_count - existing_id_count
    print(
        f"\n{dt.now()} page_unique_count: {loop_unique_count} currentCount: {all_unique_count}"
    )
    return all_unique


def zero_count(loop_count):
    """Returns false when 0 new ids returned twice in a row.
    Used to end scraping"""
    if len(loop_count) <= 2:
        return True
    if loop_count[-1] + loop_count[-2] > 0:
        return True
    return False


def just_ids(keyw, locat, api, da, folder_path):
    """Assembles methods for IDs.  Save and count all.  Keyboard interupt causes end and save"""
    current_id_list = dd([])
    loop_count_list = []
    page_count = 0
    file_name = file_name_ids(folder_path, keyw, locat)
    # breaks loop when 2 consecutive loops return 0 new ids
    try:
        while zero_count(loop_count_list):
            try:
                last_id = current_id_list[-1]
            except IndexError:
                last_id = str()
            try:
                r = calling(keyw, locat, da, api, last_id, page_count)
            except urllib3.exceptions.ReadTimeoutError as e:
                print(f"{e}")
                j_dump(file_name, current_id_list)
            newest_ids = get_new_ids(r, file_name, current_id_list)
            try:
                loop_count = len(newest_ids) - len(current_id_list)
            except TypeError:
                loop_count = len(newest_ids)
            loop_count_list.append(loop_count)
            page_count += 1
            for new in newest_ids:
                current_id_list.append(new)
    except KeyboardInterrupt:
        pass
    j_dump(file_name, dd(current_id_list))
    print(
        f"Finished!  New id count is {len(dd(current_id_list))} \
        Saved to: {file_name}"
    )

    # snot


def df_to_table(
    df,
    sf_table_name,
    sf_account,
    sf_user,
    sf_password,
    sf_database,
    sf_warehouse,
    sf_schema,
) -> dict:
    """Appends df to snowflake table or creates new table"""
    with snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
    ) as conn:
        str_df = df.astype(str)
        success, chunks, rows, snowflake_output = write_pandas(
            conn=conn, df=str_df, table_name=sf_table_name, auto_create_table=True
        )
    if success is True:
        print(f"Success!  {rows} rows added to {sf_table_name} in {chunks} chunks")
    else:
        print(f"DID NOT WORK! {sf_table_name} \n {snowflake_output}")


def current_ids(
    sf_table_name,
    sf_account,
    sf_user,
    sf_password,
    sf_database,
    sf_warehouse,
    sf_schema,
):
    """retrieves existing ids to avoid duplication"""
    with snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f'select "job_id" from {sf_table_name}')
            id_list = []
            for col1 in cur:
                idee = col1[0]
                id_list.append(idee)
    return id_list


def get_job_dict(id_tuple, api):
    """Makes api call and converts html response to soup,
    extracts specific json, converts to dict return"""
    keyw = id_tuple[0]
    locat = id_tuple[1]
    id_number = id_tuple[2]
    url = id_tuple[3]
    r = scrape(3, api, url).text  ##############################
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
                "jobKeyword": keyw,
                "jobSearchLocation": locat,
            }
        )
        # print(f'{dt.now()}get_job_dict try')
        return d_soup
    except AttributeError:
        # print(f'{dt.now()}get_job_dict ex')
        return {
            "jobId": id_number,
            "jobKeyword": keyw,
            "jobSearchLocation": locat,
        }


# Ids ready
def ids_warming(
    path,
    sf_table_name,
    sf_account,
    sf_user,
    sf_password,
    sf_database,
    sf_warehouse,
    sf_schema,
):
    """Extract ids from json files.  Ouput list of tuples with keyword, location, id, URL"""
    id_tuple_list = []
    # get ids that have already run
    df_ids = current_ids(
        sf_table_name,
        sf_account,
        sf_user,
        sf_password,
        sf_database,
        sf_warehouse,
        sf_schema,
    )
    # list of id file names
    ids = glob.glob(f"{path}/indeedIds_*.json")
    # loop through file names extract kw and location
    for d in ids:
        id_path = d.split("/")[-1]
        id_name = id_path.split("_")
        kw = id_name[1]
        loc = id_name[2].split(".")[0]
        # open each id file
        with open(d, "r", encoding="utf-8") as f:
            j_ids = json.load(f)[d]
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
    tuple_dd_list = dd(id_tuple_list)
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


# spac
def spacy_proper(doc):
    """Takes string as input and returns a list of Proper Nouns"""
    pn_list = []
    for tok in doc:
        if tok.pos_ == "PROPN":
            pn_list.append(tok.text)
        else:
            pass
    return pn_list


def sentence_parse_proper(sentences):
    """Parses list of sentences and returns list of proper nouns"""
    col_lists = []
    try:
        for sentence in sentences:
            ss = sentence.strip()
            doc = nlp(ss)
            pn = spacy_proper(doc)
            col_lists.append(pn)
    except ValueError:
        col_lists.append([])
    return set(chain.from_iterable(col_lists))


def pattern_lower(csv_file, column_name):
    """Formats column from csv file into patterns for matching"""
    pattern_list = []
    df = pd.read_csv(csv_file)
    word_list = list(df[column_name])
    clean_word_list = [str(t).lower().strip() for t in word_list if t is not np.nan]
    split_list = [t.split() for t in clean_word_list]
    for i in range(len(split_list)):
        words = []
        sentence = split_list[i]
        for w in sentence:
            pattern = dict(LOWER=str(w))
            words.append(pattern)
        pattern_list.append(words)
    # print(f'{column_name} now contains {len(pattern_list)} keywords')
    return pattern_list


def data_word_match(sentence, csv_file, column_name):
    """Matches phrase patterns"""
    matcher = Matcher(nlp.vocab)
    word_patterns = pattern_lower(csv_file, column_name)
    matcher.add(column_name, word_patterns, greedy="FIRST")
    doc = nlp(sentence)
    matches = matcher(doc)
    words = []
    for match_id, start, end in matches:  ##match_id, not used but do not remove
        span = doc[start:end]
        words.append(span.text)
    return list(words)


# TODO: use lemmatization to match base words instead of exact # [fixme]


def sentence_parse_data_words(sentences, csv_file, column_name):
    """Takes string as input and returns a list of Proper Nouns"""
    words_lists = []
    try:
        for sentence in sentences:
            words = data_word_match(sentence, csv_file, column_name)
            words_lists.append(words)
    except ValueError:
        words_lists.append([])
    return set(chain.from_iterable(words_lists))


def check_and_extract(full_dict):
    """This function is designed to catch errors in the dictionary.
    Function li_li_li_list extracts list of values.  Dict with data and lables returned
    """
    try:
        company = full_dict["hiringOrganization"]["name"]
    except KeyError:
        company = "Unavailable"
    except TypeError:
        company = "Unavailable"
    try:
        title = full_dict["title"]
    except TypeError:
        title = "Unavailable"
    except KeyError:
        title = "Unavailable"
    try:
        base_salary_low = full_dict["baseSalary"]["value"]["minValue"]
    except KeyError:
        base_salary_low = "Unavailable"
    except TypeError:
        base_salary_low = "Unavailable"
    try:
        base_salary_high = full_dict["baseSalary"]["value"]["maxValue"]
    except KeyError:
        base_salary_high = "Unavailable"
    except TypeError:
        base_salary_high = "Unavailable"
    try:
        salary_period = full_dict["baseSalary"]["value"]["unitText"]
    except KeyError:
        salary_period = "Unavailable"
    except TypeError:
        salary_period = "Unavailable"
    try:
        date_posted = full_dict["datePosted"]
    except KeyError:
        date_posted = "Unavailable"
    except TypeError:
        date_posted = "Unavailable"
    # try:
    #     date_expires = full_dict["date_expires"]
    # except TypeError:
    #     date_expires = "Unavailable"
    # except KeyError:
    #     date_expires = "Unavailable"
    try:
        employment_type = full_dict["employmentType"]
    except KeyError:
        employment_type = "Unavailable"
    except TypeError:
        employment_type = "Unavailable"
    try:
        locat = full_dict["jobLocation"]["address"]["addressLocality"]
    except KeyError:
        locat = "Unavailable"
    except TypeError:
        locat = "Unavailable"
    try:
        description_raw = full_dict["description"]
    except KeyError:
        description_raw = "Unavailable"
    except TypeError:
        description_raw = "Unavailable"
    try:
        description_list = li_li_li_list(description_raw)
    except KeyError:
        description_list = "Unavailable"
    except TypeError:
        description_list = "Unavailable"
    skills = sentence_parse_data_words(
        description_list, os.getenv("MATCH_WORDS"), os.getenv("MATCH_COLUMN_SKILLS")
    )
    skills = [x.lower() for x in skills]
    technology = sentence_parse_data_words(
        description_list, os.getenv("MATCH_WORDS"), os.getenv("MATCH_COLUMN_TECHNOLOGY")
    )
    data_technology = [x.lower() for x in technology]
    propers = sentence_parse_proper(description_list)
    proper_nouns = [
        x.lower()
        for x in propers
        if x.lower() not in skills and x.lower() not in data_technology
    ]
    job_id = full_dict["jobId"]
    keyw = full_dict["jobKeyword"]
    search_location = full_dict["jobSearchLocation"]
    print(
        f"{job_id}  {keyw}   {search_location}  {company}  {title}  {date_posted}\
          {employment_type}  {locat}  \n  {proper_nouns} \n {skills} \n {data_technology}"
    )
    job_dict = {
        "job_id": job_id,
        "entered": time.strftime("%D %T"),
        "search_keyword": keyw,
        "search_location": search_location,
        "job_company": company,
        "job_title": title,
        "job_date_posted": date_posted,
        # "job_date_expires": date_expires,
        "pay_low": base_salary_low,
        "pay_high": base_salary_high,
        "pay_period": salary_period,
        "job_type": employment_type,
        "job_location": locat,
        "description_sentences": description_list,
        "proper_nouns": proper_nouns,
        "data_skills": skills,
        "data_technology": data_technology,
    }
    return job_dict


def come_together(
    path,
    sf_table_name,
    api,
    sf_account,
    sf_user,
    sf_password,
    sf_database,
    sf_warehouse,
    sf_schema,
):
    """runs scraped ids and returns list df"""
    dict_list = []
    tup_list = ids_warming(
        path,
        sf_table_name=sf_table_name,
        sf_account=sf_account,
        sf_user=sf_user,
        sf_password=sf_password,
        sf_database=sf_database,
        sf_warehouse=sf_warehouse,
        sf_schema=sf_schema,
    )  # returns list of id tuples  ->list
    er_count = 0
    try:
        for tup in tup_list:
            try:
                job_dict = get_job_dict(
                    tup, api
                )  # takes one id tuple and outputs dict with data -> dict
                new_job_dict = check_and_extract(
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
        df_n = pd.DataFrame(dict_list)
        df_to_table(
            df=df_n,
            sf_table_name=sf_table_name,
            sf_account=sf_account,
            sf_user=sf_user,
            sf_password=sf_password,
            sf_database=sf_database,
            sf_warehouse=sf_warehouse,
            sf_schema=sf_schema,
        )
        return df_n
    except KeyboardInterrupt:
        df_n = pd.DataFrame(dict_list)
        df_to_table(
            df_n,
            sf_table_name,
            sf_account,
            sf_user,
            sf_password,
            sf_database,
            sf_warehouse,
            sf_schema,
        )
        return df_n


if __name__ == "__main__":
    print("Running job_words.py directly")
else:
    print("Loading job_words.py")
