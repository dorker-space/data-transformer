SELECT COUNT(*) from jobs;

CREATE VIRTUAL TABLE jobs_fts USING fts5(title, description, content='jobs');

INSERT INTO jobs_fts(rowid, title, description)
SELECT id, title, description FROM jobs;


SELECT COUNT(DISTINCT location) FROM jobs
WHERE location LIKE '%Remote%';

SELECT location, COUNT(*) as cnt
FROM jobs
WHERE location LIKE '%remote%'
GROUP BY location
ORDER BY cnt DESC;


ALTER TABLE jobs ADD COLUMN is_remote TEXT;

SELECT j.id, jobs_fts.title, j.company_slug, j.location, jobs_fts.description, j.url, j.posted_at, j.is_remote
FROM jobs_fts
         JOIN jobs j ON j.id = jobs_fts.ROWID
WHERE (
    j.is_remote = 'KEEP_GLOBAL' OR
    j.is_remote = 'KEEP_PURE' OR
    j.is_remote = 'POTENTIAL_PURE')
  AND jobs_fts MATCH '
                     (Golang OR "Go engineer" OR "Go developer" OR "Go backend" OR "Go language" OR "Go stack")
                     NOT title : (Lead OR Principal OR Staff OR Director OR Architect OR Manager OR VP OR Head OR "Senior-Lead")
                     NOT title : Frontend'
  AND j.posted_at >= date('now', '-1 month')
ORDER BY j.posted_at DESC;

CREATE INDEX jb_posted_date ON jobs(posted_at);

SELECT
    COUNT(CASE WHEN posted_at <= DATE('now', '-1 month') THEN 1 END) AS jobs_older_than_month,

    COUNT(*) AS all_jobs,

    (COUNT(CASE WHEN posted_at <= DATE('now', '-1 month') THEN 1 END) * 100.0) / COUNT(*) AS percentage
FROM jobs;

SELECT MIN(posted_at) FROM jobs;

SELECT title, url FROM jobs
WHERE posted_at = '2026-01-01 00:00:00.000000';

SELECT MIN(posted_at) AS true_earliest_date
FROM jobs
WHERE posted_at > '2026';

SELECT j.id, jobs_fts.title, j.company_slug, j.location, jobs_fts.description, j.url, j.posted_at, j.is_remote
FROM jobs_fts
         JOIN jobs j ON j.id = jobs_fts.ROWID
WHERE (
    j.is_remote = 'KEEP_GLOBAL' OR
    j.is_remote = 'KEEP_PURE' OR
    j.is_remote = 'POTENTIAL_PURE')
  AND jobs_fts MATCH '
                     (Python OR "AI engineer")
                     NOT ("MLOps" OR "Data scientist")
                     NOT title : (Lead OR Principal OR Staff OR Director OR Architect OR Manager OR VP OR Head OR "Senior-Lead")'
  AND j.posted_at >= date('now', '-1 month')
ORDER BY j.posted_at DESC;


CREATE TABLE test_golden_set (
                                 job_id INTEGER PRIMARY KEY,
                                 title TEXT,
                                 description TEXT,
                                 location TEXT,
                                 ground_truth INTEGER DEFAULT NULL
);

INSERT INTO test_golden_set (job_id, title, description, location)
SELECT id, title, description, location
FROM jobs
ORDER BY RANDOM()
    LIMIT 1000;

SELECT title FROM test_golden_set
WHERE title LIKE '%engineer%' or title LIKE '%software';

SELECT title FROM test_golden_set;


SELECT count( location)
FROM  jobs
WHERE is_remote = 'KEEP_GLOBAL';


SELECT id, title, description FROM jobs
WHERE location = 'Bay Area (hybrid/remote)';

SELECT * from jobs
LIMIT 1;