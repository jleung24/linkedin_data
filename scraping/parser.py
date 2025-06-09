from bs4 import BeautifulSoup


def process_s3():
    pass

def parse_job_html(html: str):
    job_data = {}
    job_soup = BeautifulSoup(job_response.read(), 'html.parser')

    title = job_soup.select_one("h2.top-card-layout__title").get_text(strip=True)
    company = job_soup.select_one("a.topcard__org-name-link").get_text(strip=True)
    location = job_soup.select_one(".topcard__flavor--bullet").get_text(strip=True)
    description = job_soup.select_one(".show-more-less-html__markup").get_text(strip=True)

    try:
        salary = job_soup.select_one(".compensation__salary").get_text(strip=True)
    except:
        salary = None
    

    job_data[job_id]["title"] = title
    job_data[job_id]["company"] = company
    job_data[job_id]["location"] = location
    job_data[job_id]["description"] = description
    job_data[job_id]["salary"] = salary

    if "Remote" in job_soup:
        print(job_soup)