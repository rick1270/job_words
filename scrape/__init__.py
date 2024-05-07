"""Connection to api and scraping"""

import requests
import names

# Uses proxies and redirects to automatically scrape id html.  Requires key.
def scrape(retries, api_key, url):
    """request and return html from job listing page with retries
    and results printed.  Single thread.  Returned html is used to
    make next url.  Returns r"""
    tries = 0
    while tries <= retries:
        scraper_api = api_key
        payload = {"api_key": scraper_api, "url": url}
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
            tries += 1
        print(rf"Try {tries} of {retries} n\ {r.headers}")
    return print(f"{url} has problems")


def calling(keyword, location, days, api_key, last_id, page_count):
    """Bundles methods and returns id html."""
    id_url = names.url_next(keyword, location, days, last_id, page_count)
    r = scrape(3, api_key, id_url)
    return r
