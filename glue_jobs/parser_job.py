import sys
import os
import zipfile
from datetime import date, timedelta

# Add the zip to sys.path
added = False
for path in sys.path:
    if path.startswith("/tmp/glue-python-libs-") and os.path.isdir(path):
        for fname in os.listdir(path):
            if fname.endswith(".zip"):
                zip_path = os.path.join(path, fname)
                sys.path.insert(0, zip_path)
                print(f"Added {zip_path} to sys.path")
                # List contents
                with zipfile.ZipFile(zip_path, 'r') as z:
                    print("Zip contents:", z.namelist())
                added = True
                break
        if added:
            break

print("Before import linkedin_data")
try:
    import linkedin_data
    print("linkedin_data imported successfully")
except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
    raise

import psycopg2
import pandas as pd
import boto3
import logging
from linkedin_data.scraping.parser import process_s3
from linkedin_data.helpers.read_config import read_config
from linkedin_data.helpers import logger

logger = logging.getLogger('scraping_logger')
rds_config = read_config('redshift')
yesterday = date.today() - timedelta(days=1)
parsed_job_data = process_s3(yesterday)

redshift_user = rds_config['username']
redshift_password = rds_config['password']

conn = psycopg2.connect(
    host="default-workgroup.385122037390.us-west-1.redshift-serverless.amazonaws.com",
    port=5439,
    dbname="job_data",
    user=redshift_user,
    password=redshift_password
)

cur = conn.cursor()
cur.execute("SELECT skill_id, name FROM skills")
skills_rows = cur.fetchall()
skill_name_to_id = {row[1]: row[0] for row in skills_rows}
cur.close()
conn.close()

jobs_data = []
salary_data = []
job_skills_data = []

for job_id, job in parsed_job_data.items():
    logger.info(f"Processing job id:    {job_id}")
    jobs_data.append([
        job_id, job["title"], job["company"], job["location"], job["level"],
        job["years_experience_min"], job["years_experience_max"], job["url"], job["date"]
    ])
    if job["salary_min"] is not None:
        salary_data.append([
            job_id, job["salary_min"], job["salary_max"], job["salary_unit"], "USD"
        ])
    for skill in job["found_skills"]:
        skill_id = skill_name_to_id.get(skill)
        if skill_id:
            job_skills_data.append([job_id, skill_id])

logger.info("Done processing jobs...")

jobs_df = pd.DataFrame(jobs_data, columns=[
    'job_id', 'title', 'company', 'location', 'level',
    'years_experience_min', 'years_experience_max', 'url', 'posted_date'
])
for col in ['years_experience_min', 'years_experience_max']:
    jobs_df[col] = jobs_df[col].apply(lambda x: int(x) if pd.notnull(x) and x != '' else '')

salary_df = pd.DataFrame(salary_data, columns=[
    'job_id', 'amount_min', 'amount_max', 'time_unit', 'currency'
])

job_skills_df = pd.DataFrame(job_skills_data, columns=['job_id', 'skill_id'])

logger.info("Done creating data frames")

s3 = boto3.client('s3')
bucket = 'temp817'
prefix = 'redshift_load/'

jobs_csv = '/tmp/jobs.csv'
jobs_df.to_csv(jobs_csv, index=False)
s3.upload_file(jobs_csv, bucket, f'{prefix}jobs.csv')

salary_csv = '/tmp/salary.csv'
salary_df.to_csv(salary_csv, index=False)
s3.upload_file(salary_csv, bucket, f'{prefix}salary.csv')

job_skills_csv = '/tmp/job_skills.csv'
job_skills_df.to_csv(job_skills_csv, index=False)
s3.upload_file(job_skills_csv, bucket, f'{prefix}job_skills.csv')

logger.info("Done uploading to S3")

iam_role = 'arn:aws:iam::385122037390:role/service-role/AmazonRedshift-CommandsAccessRole-20250618T175826'  

conn = psycopg2.connect(
    host="default-workgroup.385122037390.us-west-1.redshift-serverless.amazonaws.com",
    port=5439,
    dbname="job_data",
    user=rds_config['username'],
    password=rds_config['password']
)
cur = conn.cursor()

copy_jobs = f"""
COPY jobs
FROM 's3://{bucket}/{prefix}jobs.csv'
IAM_ROLE '{iam_role}'
FORMAT AS CSV
IGNOREHEADER 1;
"""
copy_salary = f"""
COPY salary
FROM 's3://{bucket}/{prefix}salary.csv'
IAM_ROLE '{iam_role}'
FORMAT AS CSV
IGNOREHEADER 1;
"""
copy_job_skills = f"""
COPY job_skills
FROM 's3://{bucket}/{prefix}job_skills.csv'
IAM_ROLE '{iam_role}'
FORMAT AS CSV
IGNOREHEADER 1;
"""

cur.execute(copy_jobs)
cur.execute(copy_salary)
cur.execute(copy_job_skills)
conn.commit()
cur.close()
conn.close()

