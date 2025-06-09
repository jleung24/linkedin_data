import urllib.request
from urllib.request import OpenerDirector
import random
import time

from bs4 import BeautifulSoup

from helpers.read_config import read_config


proxy_config = read_config("oxylabs")

def get_proxy():
    username = proxy_config["username"]
    password = proxy_config["password"]

    proxy = (
        'http://customer-%s-cc-US:%s@pr.oxylabs.io:7777' %
        (username, password)
    )
    
    return proxy

def scrape_jobs(pages: int):
    proxy = get_proxy()
    proxy_handler = urllib.request.ProxyHandler({
        "http": proxy,
        "https": proxy
    })

    opener = urllib.request.build_opener(proxy_handler)
    opener.addheaders = [("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36")]
    
    for i in range(0, pages * 25, 25):
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Software%20Engineer&location=United%20States&start={i}&f_TPR=r604800"
        response = opener.open(url)
        scrape_page(response.read(), opener)
        time.sleep(random.uniform(2, 5))

def scrape_page(html: str, opener: OpenerDirector):
    soup = BeautifulSoup(html, 'html.parser')
    for job_card in soup.find_all('div', class_='base-card'):
        urn = job_card.get('data-entity-urn')
        if urn and 'jobPosting' in urn:
            job_id = urn.split(':')[-1]
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
            job_response = opener.open(url)

            # push to S3
        
        time.sleep(random.uniform(2, 5))

scrape_jobs(5)