"""Stores and retrieves local files"""

import json

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
