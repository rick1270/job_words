"""Modules to retrieve and store Indeed Ids"""

from datetime import datetime as dt
from bs4 import BeautifulSoup
import store_retrieve as sr
import names
import scrape
import urllib3



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
        existing_ids_load = sr.j_load(file_name)
        existing_id_count = existing_ids_load[0]
        existing_ids = existing_ids_load[1]
        existing_id_dd = sr.dd(existing_ids)
        print(f"starting id count is {existing_ids_load[0]}")
    else:
        existing_id_dd = sr.dd(old_ids)
        existing_id_count = len(existing_id_dd)
    try:
        loop_id_list = get_ids(r)
        all_unique = sr.dd(existing_id_dd + loop_id_list)
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


def just_ids(keyword=None, location=None, api_key=None, days=None, folder_path=None):
    """Assembles methods for IDs.  Save and count all.  Keyboard interupt causes end and save"""
    current_id_list = sr.dd([])
    loop_count_list = []
    page_count = 0
    file_name = names.file_name_ids(folder_path, keyword, location)
    # breaks loop when 2 consecutive loops return 0 new ids
    try:
        while zero_count(loop_count_list):
            try:
                last_id = current_id_list[-1]
            except IndexError:
                last_id = str()
            try:
                r = scrape.calling(keyword, location, days, api_key, last_id, page_count)
            except urllib3.exceptions.ReadTimeoutError as e:
                print(f"{e}")
                sr.j_dump(file_name, current_id_list)
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
    sr.j_dump(file_name, sr.dd(current_id_list))
    print(
        f"Finished!  New id count is {len(sr.dd(current_id_list))} \
        Saved to: {file_name}"
    )
