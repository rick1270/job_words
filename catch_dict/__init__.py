"""Handles errors and assembles job dict"""

import time
import extract
import spac


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
    try:
        date_expires = full_dict["date_expires"]
    except TypeError:
        date_expires = "Unavailable"
    except KeyError:
        date_expires = "Unavailable"
    try:
        employment_type = full_dict["employmentType"]
    except KeyError:
        employment_type = "Unavailable"
    except TypeError:
        employment_type = "Unavailable"
    try:
        location = full_dict["jobLocation"]["address"]["addressLocality"]
    except KeyError:
        location = "Unavailable"
    except TypeError:
        location = "Unavailable"
    try:
        description_raw = full_dict["description"]
    except KeyError:
        description_raw = "Unavailable"
    except TypeError:
        description_raw = "Unavailable"
    try:
        description_list = extract.li_li_li_list(description_raw)
    except KeyError:
        description_list = "Unavailable"
    except TypeError:
        description_list = "Unavailable"
    skills = spac.sentence_parse_data_words(
        description_list, "Data/snow_words.csv", "Data_Skills"
    )
    data_skills = [x.lower() for x in skills]
    technology = spac.sentence_parse_data_words(
        description_list, "Data/snow_words.csv", "Data_Technology"
    )
    data_technology = [x.lower() for x in technology]
    propers = spac.sentence_parse_proper(description_list)
    proper_nouns = [
        x.lower()
        for x in propers
        if x.lower() not in data_skills and x.lower() not in data_technology
    ]
    job_id = full_dict["jobId"]
    keyword = full_dict["jobKeyword"]
    search_location = full_dict["jobSearchLocation"]
    print(
        f"{job_id}  {keyword}   {search_location}  {company}  {title}  {date_posted}\
          {employment_type}  {location}  \n  {proper_nouns} \n {data_skills} \n {data_technology}"
    )
    job_dict = {
        "job_id": job_id,
        "entered": time.strftime("%D %T"),
        "search_keyword": keyword,
        "search_location": search_location,
        "job_company": company,
        "job_title": title,
        "job_date_posted": date_posted,
        "job_date_expires": date_expires,
        "pay_low": base_salary_low,
        "pay_high": base_salary_high,
        "pay_period": salary_period,
        "job_type": employment_type,
        "job_location": location,
        "description_sentences": description_list,
        "proper_nouns": proper_nouns,
        "data_skills": data_skills,
        "data_technology": data_technology,
    }
    return job_dict
