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

from linkedin_data.scraping.parser import process_s3
from linkedin_data.helpers.read_config import read_config
import psycopg2

rds_config = read_config('redshift')
parsed_job_data = process_s3(date.today())

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

for job_id, job in parsed_job_data.items():
    # Insert job
    cur.execute("""
        INSERT INTO jobs (job_id, title, company, location, level, years_experience_min, years_experience_max, url, posted_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (job_id) DO NOTHING
    """, (
        job_id, job["title"], job["company"], job["location"], job["level"],
        job["years_experience_min"], job["years_experience_max"], job["url"], job["date"]
    ))

    # Insert salary
    cur.execute("""
        INSERT INTO salary (job_id, amount_min, amount_max, time_unit, currency)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (job_id) DO NOTHING
    """, (
        job_id, job["salary_min"], job["salary_max"], job["salary_unit"], "USD"
    ))

    # Insert job_skills
    for skill in job["found_skills"]:
        skill_id = skill_name_to_id.get(skill)
        if skill_id:
            cur.execute("""
                INSERT INTO job_skills (job_id, skill_id)
                VALUES (%s, %s)
                ON CONFLICT (job_id, skill_id) DO NOTHING
            """, (job_id, skill_id))

conn.commit()
cur.close()
conn.close()