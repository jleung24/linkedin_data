import random
import time
import logging
import urllib.request
import threading
from datetime import date
from urllib.request import OpenerDirector
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

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

def proxy_request(url: str, max_retries: int = 3) -> bytes:
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(random.uniform(2, 5))
            opener = build_random_opener() 
            with opener.open(url) as response:
                return response.read()

        except Exception as e:
            logger.error(f"Attempt {attempt} failed for {url}: {e}")
            last_exception = e
            time.sleep(random.uniform(5, 10))

    raise last_exception

def scrape_jobs(max_workers=10, job_workers=40, jobs_per_page=25, max_pages=100):
    queue = Queue()
    queue.put(0)
    visited = set()
    lock = threading.Lock()

    def worker(job_executor):
        while True:
            try:
                start = queue.get(timeout=3)
            except Empty:
                return

            with lock:
                if start in visited or (start // jobs_per_page) >= max_pages:
                    queue.task_done()
                    continue
                visited.add(start)

            try:
                html_str = scrape_jobs_by_index(start)
            except Exception as e:
                logger.error(f"Error scraping page starting at {start}: {e}")
                queue.task_done()
                continue

            jobs_found = scrape_page(html_str, job_executor)
            if jobs_found:
                queue.put(start + jobs_per_page)

            queue.task_done()

    with (
        ThreadPoolExecutor(max_workers=job_workers) as job_executor,
        ThreadPoolExecutor(max_workers=max_workers) as page_executor,
    ):
        for _ in range(max_workers):
            page_executor.submit(worker, job_executor)
        queue.join()

def scrape_jobs_by_index(i: int):
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Software%20Engineer&location=United%20States&start={i}&f_TPR=r86400"
    response = proxy_request(url)
    return response.decode('utf-8')

def scrape_page(html_str: str, job_executor):
    soup = BeautifulSoup(html_str, 'html.parser')
    job_cards = soup.find_all('div', class_='base-card')
    jobs_found = len(job_cards) > 0

    for job_card in job_cards:
        job_executor.submit(process_job_card, job_card)

    return jobs_found

def process_job_card(job_card):
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

if __name__ == "__main__":
    scrape_jobs()
