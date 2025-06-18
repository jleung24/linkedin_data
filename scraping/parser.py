import re
from datetime import date, timedelta

from bs4 import BeautifulSoup

from linkedin_data.helpers.read_config import read_config
from linkedin_data.database.s3_client import get_html


S3_BUCKET_NAME = read_config("s3")["bucket_name"]
SKILLS_KEYWORDS = [
    # Programming Languages
    "Python", "Java", "C#", "C++", "Go", "Golang", "JavaScript", "Rust", "Scala", "SQL",
    # Frameworks & Libraries
    ".NET", "ASP.Net", "Spring", "Django", "Flask", "React", "Vue", "Angular",
    # Databases & Data Warehousing
    "SQL Server", "Oracle", "PostgreSQL", "MySQL", "MongoDB", "NoSQL",
    "Redshift", "BigQuery", "Snowflake", "Synapse Analytics", "Teradata", "Aurora",
    # ETL & Data Integration Tools
    "ETL", "Airflow", "NiFi", "Talend", "Informatica", "SSIS", "Glue", "dbt", "DataStage", "Fivetran", "Matillion",
    # Cloud Platforms & Services
    "Azure", "Google Cloud Platform", "GCP", "Amazon Web Services", "AWS",
    "S3", "Lambda", "EMR", "RDS", "Kinesis", "DynamoDB", "Athena", "EC2", "CloudFormation",
    "Data Factory", "Databricks", "Google Cloud Dataflow", "Google Cloud Storage", "Google Cloud Pub/Sub",
    # DevOps & Infrastructure
    "Docker", "Kubernetes", "CloudFormation", "Terraform", "Azure Pipelines", "Puppet", "Chef", "Ansible", "Jenkins", "GitLab CI/CD", "ADO", "Podman",
    # APIs & Web Technologies
    "RESTful APIs", "GraphQL", "HTML", "CSS",
    # Tools & Platforms
    "Git", "GitLab", "Jira", "Confluence", "SysML", "ORM", "Test Management Tools",
    # Methodologies & Practices
    "Agile", "DevOps", "DevSecOps", "Waterfall", "CI/CD",
    # Data Engineering & Analytics
    "Data Warehousing", "Hadoop", "Spark", "Big Data Systems", "Business Intelligence", "Analytics", "Machine Learning"
]

KEYWORDS = r'(?:experience|development|engineering|engineer)'
YEARS_PATTERNS = [
    r'(\d+)(?:\))?\s*(?:\+)?\s*(?:years?|yrs\.?|yr\.?)(?:[^\.\n]{0,40}?)' + KEYWORDS,
    r'(one|two|three|four|five|six|seven|eight|nine|ten)\s+years?(?:[^\.\n]{0,40}?)' + KEYWORDS,
    r'(\d+)\s*[-–]\s*(\d+)\s*(?:years?|yrs\.?|yr\.?)(?:[^\.\n]{0,40}?)' + KEYWORDS,                # e.g., 2-4 years, 5–7 yrs
    r'(\d+)\s*(?:to|and|through)\s*(\d+)\s*(?:years?|yrs\.?|yr\.?)(?:[^\.\n]{0,40}?)' + KEYWORDS,  # e.g., 3 to 5 years, 1 and 3 yrs
    r'between\s+(\d+)\s+and\s+(\d+)\s*(?:years?|yrs\.?|yr\.?)(?:[^\.\n]{0,40}?)' + KEYWORDS,       # e.g., between 1 and 3 years
    r'minimum\s+(\d+)\s*(?:years?|yrs\.?|yr\.?).*maximum\s+(\d+)\s*(?:years?|yrs\.?|yr\.?)(?:[^\.\n]{0,40}?)' + KEYWORDS, # e.g., minimum 2 years, maximum 5 years
]
WORD_TO_NUM = {
    'one': '1',
    'two': '2',
    'three': '3',
    'four': '4',
    'five': '5',
    'six': '6',
    'seven': '7',
    'eight': '8',
    'nine': '9',
    'ten': '10'
}


def process_s3(folder_date: str) -> dict:
    job_data = get_html(S3_BUCKET_NAME, folder_date)
    parsed_job_data = {}
    for key, value in job_data.items():
        job_id = key.split("/")[1]
        key_date = key.split("/")[0]
        try:
            parse_job_html(value, job_id, key_date, parsed_job_data)
        except:
            continue
    
    return parsed_job_data

def parse_job_html(html: str, job_id: str, date: str, parsed_job_data: dict):
    job_soup = BeautifulSoup(html, 'html.parser')
    
    title = job_soup.select_one("h2.top-card-layout__title").get_text(strip=True)
    company = job_soup.select_one("a.topcard__org-name-link").get_text(strip=True)
    location = job_soup.select_one(".topcard__flavor--bullet").get_text(strip=True)
    description = job_soup.select_one(".show-more-less-html__markup").get_text(strip=True)
    level = job_soup.select_one(".description__job-criteria-text").get_text(strip=True)

    title = re.sub(r'[^\x20-\x7E]', '', title)
    if len(title) > 100:
        title = title[:100]
    
    company = re.sub(r'[^\x20-\x7E]', '', company)
    if len(company) > 100:
        company = company[:100]

    code_tag = job_soup.select_one("#joinUrlWithRedirect")
    if code_tag and code_tag.string:
        match = re.search(r'"(https://[^"]+)"', code_tag.string)
        if match:
            url = match.group(1)
        else:
            url = None

    try:
        salary = job_soup.select_one(".compensation__salary").get_text(strip=True)
        salary_min = cleanup_salary(salary.split(" - ")[0])
        salary_max = cleanup_salary(salary.split(" - ")[1])
        salary_unit = salary.split(" - ")[0].split("/")[1]
    except:
        salary_max = salary_min = salary_unit = None
    
    found_skills = []
    if description:
        found_skills = [skill for skill in SKILLS_KEYWORDS if skill.lower() in description.lower()]

    years_experience_min = None
    years_experience_max = None
    for pattern in YEARS_PATTERNS:
        matches = re.findall(pattern, description, flags=re.IGNORECASE)
        if matches:
            has_tuple = isinstance(matches[0], tuple)
            if has_tuple:
                matches = flatten_list_of_tuple(matches)

            normalized = [WORD_TO_NUM.get(m.lower(), m) for m in matches]
            
            for num in normalized:
                years = int(num)
                if years >= 15:
                    continue
                if years_experience_min is None or years < years_experience_min:
                    years_experience_min = years
                
                if years_experience_max is None or years > years_experience_max:
                    years_experience_max = years

    parsed_job_data[job_id] = {}
    parsed_job_data[job_id]['title'] = title
    parsed_job_data[job_id]['company'] = company
    parsed_job_data[job_id]['location'] = location
    parsed_job_data[job_id]['level'] = level
    parsed_job_data[job_id]['url'] = url
    parsed_job_data[job_id]['years_experience_min'] = years_experience_min
    parsed_job_data[job_id]['years_experience_max'] = years_experience_max
    parsed_job_data[job_id]['salary_min'] = salary_min
    parsed_job_data[job_id]['salary_max'] = salary_max
    parsed_job_data[job_id]['salary_unit'] = salary_unit
    parsed_job_data[job_id]['found_skills'] = found_skills
    parsed_job_data[job_id]['date'] = date
   
def flatten_list_of_tuple(tuple_list: list) -> list:
    flat_list = []
    for current_tuple in tuple_list:
        flat_list.append(current_tuple[0])
        flat_list.append(current_tuple[1])

    return flat_list

def cleanup_salary(salary_string: str) -> int:
    match = re.search(r"[\d,]+(?:\.\d+)?", salary_string)
    salary_int = float(match.group().replace(',', ''))
    return salary_int

if __name__ == "__main__":
    process_s3(date.today())
