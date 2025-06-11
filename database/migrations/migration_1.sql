CREATE TABLE jobs (
    job_id INTEGER PRIMARY KEY,
    title VARCHAR(100),
    company VARCHAR(50),
    location VARCHAR(50),
    level VARCHAR(50),
    years_experience_min INTEGER,
    years_experience_max INTEGER
);

CREATE TABLE skills (
    skill_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE salary (
    salary_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(job_id),
    amount_min NUMERIC(12,2) NOT NULL,
    amount_max NUMERIC(12,2) NOT NULL,
    time_unit VARCHAR(10) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD'
);

CREATE TABLE job_skills (
    job_id INTEGER NOT NULL,
    skill_id INTEGER NOT NULL,
    PRIMARY KEY (job_id, skill_id),
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    FOREIGN KEY (skill_id) REFERENCES skills(skill_id)
);
