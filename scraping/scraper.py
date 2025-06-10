import random
import time
import logging
import urllib.request
from datetime import date
from urllib.request import OpenerDirector

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from helpers.read_config import read_config
from database.s3_client import upload_html
from helpers import logger


logger = logging.getLogger('scraping_logger')

S3_BUCKET_NAME = read_config("s3")["bucket_name"]
proxy_config = read_config("oxylabs")
ua = UserAgent()

def get_proxy() -> str:
    username = proxy_config["username"]
    password = proxy_config["password"]

    proxy = (
        'http://customer-%s-cc-US:%s@pr.oxylabs.io:7777' %
        (username, password)
    )
    
    return proxy

def build_random_opener() -> OpenerDirector:
    proxy = get_proxy()
    proxy_handler = urllib.request.ProxyHandler({
        "http": proxy,
        "https": proxy
    })

    opener = urllib.request.build_opener(proxy_handler)
    random_user_agent = ua.random
    opener.addheaders = [("User-Agent", random_user_agent)]

    return opener

def proxy_request(url: str) -> bytes:
    time.sleep(random.uniform(5, 10))
    opener = build_random_opener()
    with opener.open(url) as response:
        data = response.read()

    return data

def scrape_jobs(pages: int):
    
    for i in range(0, pages * 25, 25):
        try:
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Software%20Engineer&location=United%20States&start={i}&f_TPR=r86400"
            response = proxy_request(url)
        except Exception as e:
            logger.error(f"Error fetching job listings at start={i}: {e}")
            break
        html_str = response.decode('utf-8')
        scrape_page(html_str)

def scrape_page(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    for job_card in soup.find_all('div', class_='base-card'):
        urn = job_card.get('data-entity-urn')
        if urn and 'jobPosting' in urn:
            job_id = urn.split(':')[-1]
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
            try:
                job_response = proxy_request(url)
                html_str = job_response.decode('utf-8')
                s3_key = f"{date.today()}/{job_id}"
                upload_html(html_str, s3_key, S3_BUCKET_NAME)
            except Exception as e:
                logger.error(f"Error scraping job {job_id}: {e}")
                continue

if __name__ == "__main__":
    scrape_jobs(10)
