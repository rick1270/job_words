"""create file and url names"""

def url_next(keyword, location, days, last_id=None, page_count=0):
    """Create start url for ids if lastId = None.  Otherwise uses id from previous request"""
    url = f"https://www.indeed.com/jobs?q={keyword}&l={location}&radius=15&sort=date&fromage={days}"
    if page_count == 0:
        return url
    else:
        return f"{url}&start={page_count * 10}&vjk={last_id}"


def file_name_ids(folder_path, keyword, location):
    """creates json file name for ids using keyword and location"""
    file_name = f"{folder_path}/indeedIds_{keyword}_{location}.json"
    return file_name
