from datetime import date

from bs4 import BeautifulSoup

from helpers.read_config import read_config
from database.s3_client import get_html


S3_BUCKET_NAME = read_config("s3")["bucket_name"]

def process_s3():
    job_data = get_html(S3_BUCKET_NAME, date.today())
    for key, value in job_data.items():
        job_id = key.split("/")[1]
        key_date = key.split("/")[0]
        try:
            parse_job_html(value, job_id, key_date)
        except:
            continue

def parse_job_html(html: str, job_id: str, date: str):
    job_data = {}
    job_soup = BeautifulSoup(html, 'html.parser')
    
    title = job_soup.select_one("h2.top-card-layout__title").get_text(strip=True)
    company = job_soup.select_one("a.topcard__org-name-link").get_text(strip=True)
    location = job_soup.select_one(".topcard__flavor--bullet").get_text(strip=True)
    description = job_soup.select_one(".show-more-less-html__markup").get_text(strip=True)

    try:
        salary = job_soup.select_one(".compensation__salary").get_text(strip=True)
    except:
        salary = None
    
    job_data[job_id] = {}
    job_data[job_id]["title"] = title
    job_data[job_id]["company"] = company
    job_data[job_id]["location"] = location
    # job_data[job_id]["description"] = description
    job_data[job_id]["salary"] = salary

    print(job_data)


process_s3()