SELECT COUNT(*)
from jobs;

CREATE VIRTUAL TABLE jobs_fts USING fts5
(
    title,
    description,
    content='jobs'
);

INSERT INTO jobs_fts(rowid, title, description)
SELECT rowid, title, description
FROM jobs;


SELECT COUNT(DISTINCT location)
FROM jobs
WHERE location LIKE '%Remote%';

SELECT location, COUNT(*) as cnt
FROM jobs
WHERE location LIKE '%remote%'
GROUP BY location
ORDER BY cnt DESC;


ALTER TABLE jobs
    ADD COLUMN is_remote TEXT;

SELECT j.id, jobs_fts.title, j.company_slug, j.location, jobs_fts.description, j.url, j.posted_at
FROM jobs_fts
         JOIN jobs j ON j.id = jobs_fts.ROWID
WHERE jobs_fts MATCH '
                     (Golang OR "Go engineer" OR "Go developer" OR "Go backend" OR "Go language" OR "Go stack")
                     NOT title : (Lead OR Principal OR Staff OR Director OR Architect OR Manager OR VP OR Head OR "Senior-Lead")
                     NOT title : Frontend'
  AND j.posted_at >= date('now', '-1 month')
ORDER BY j.posted_at DESC;

SELECT COUNT(CASE WHEN posted_at <= DATE('now', '-1 month') THEN 1 END)                      AS jobs_older_than_month,

       COUNT(*)                                                                              AS all_jobs,

       (COUNT(CASE WHEN posted_at <= DATE('now', '-1 month') THEN 1 END) * 100.0) / COUNT(*) AS percentage
FROM jobs;

SELECT MIN(posted_at)
FROM jobs;

SELECT title, url
FROM jobs
WHERE posted_at = '2026-01-01 00:00:00.000000';

SELECT MIN(posted_at) AS true_earliest_date
FROM jobs
WHERE posted_at > '2026';

SELECT j.id, jobs_fts.title, j.company_slug, j.location, jobs_fts.description, j.url, j.posted_at
FROM jobs
         JOIN jobs j ON j.id = jobs_fts.ROWID
WHERE jobs_fts MATCH '
                     (Python OR "AI engineer")
                     NOT ("MLOps" OR "Data scientist")
                     NOT title : (Lead OR Principal OR Staff OR Director OR Architect OR Manager OR VP OR Head OR "Senior-Lead")'
  AND j.posted_at >= date('now', '-1 month')
ORDER BY j.posted_at DESC;


CREATE TABLE test_golden_set
(
    job_id       INTEGER PRIMARY KEY,
    title        TEXT,
    description  TEXT,
    location     TEXT,
    ground_truth INTEGER DEFAULT NULL
);

INSERT INTO test_golden_set (job_id, title, description, location)
SELECT id, title, description, location
FROM jobs
ORDER BY RANDOM()
LIMIT 1000;

SELECT title
FROM test_golden_set
WHERE title LIKE '%engineer%'
   or title LIKE '%software';

SELECT title
FROM test_golden_set;


SELECT count(location)
FROM jobs
WHERE is_remote = 'KEEP_GLOBAL';


SELECT id, title, description
FROM jobs
WHERE location = 'Bay Area (hybrid/remote)';

SELECT *
from jobs
LIMIT 1;

SELECT sql
FROM sqlite_schema
WHERE name = 'jobs';

PRAGMA foreign_keys = ON;

CREATE TABLE matches
(
    -- Keys
    job_id                     INTEGER NOT NULL,
    user_id                    INTEGER NOT NULL,

    -- Core matching constraints
    is_match                   BOOLEAN NOT NULL,                   -- false if REJECTED / true if any other
    suitability_tier           TEXT    NOT NULL,                   -- SUITABLE / STRECH / RUNWAY / REJECTED
    pipeline_status            TEXT    NOT NULL DEFAULT 'PENDING', -- PENDING / APPLIED / GHOSTED / INTERVIEWING / REJECTED (Early) / REJECTED (Late) / Declined / OFFER
    technical_capability_score REAL    NOT NULL,                   -- score [0, 1] whether candidate suitable by technologies and experience
    strategic_value_score      REAL    NOT NULL,                   -- score [0, 1] whether the company has complex challenges and seems perspective for user's growth
    strategic_reason           TEXT    NOT NULL,                   -- single sentence explaining why it matches or fails based on constraints

    -- Metadata (in case raw text was sent)
    extracted_company          TEXT    NOT NULL,
    extracted_title            TEXT    NOT NULL,
    extracted_location_status  TEXT    NOT NULL,

    -- Analytics
    analytics                  TEXT    NOT NULL,                   -- json containing arrays of pros, cons, warnings

    -- AI Suggestions
    cv_modification_points     TEXT    NOT NULL,                   -- array of strings explaining what to improve in the CV
    tailored_cover_letter      TEXT    NOT NULL,                   -- AI generated cover letter based on sentences extracted from profile.md
    application_form_answers   TEXT    NOT NUll,                   -- array of question-answer objects derived from application form. AI generated answers based on profile.md

    -- Debug
    internal_analysis_cot      TEXT    NOT NULL,                   -- json containing Chain-of-thought analysis of AI model
    internal_scoring_breakdown TEXT    NOT NULL,                   -- json containing phases deductions

    -- Technical
    created_at                 DATETIME         DEFAULT CURRENT_TIMESTAMP,
    updated_at                 DATETIME         DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (job_id, user_id),
    FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX idx_user_pipeline ON matches (user_id, pipeline_status, suitability_tier);

SELECT COUNT(id)
FROM jobs;



WITH matched_ids AS (SELECT rowid,
                            -- bm25() returns lower values for better matches in FTS5
                            bm25(jobs_fts) as rank_score
                     FROM jobs_fts
                     WHERE jobs_fts MATCH '
        ("Go" OR "Golang" OR "Python" OR "C#" OR ".NET" OR "dotnet" OR "ASP.NET" OR "C++")
        NOT title : (Lead OR Principal OR Staff OR Director OR Architect OR Manager OR VP OR Head OR Executive)
        NOT (Frontend OR "Front End" OR "UI" OR iOS OR Android OR Flutter OR "React Native" OR PHP OR WordPress OR Magento OR "Ruby on Rails" OR "Network Engineer")
    ')
SELECT j.id,
       j.title,
       j.company_slug,
       j.location,
       j.description,
       j.url,
       j.posted_at
FROM matched_ids m
         JOIN jobs j ON j.ROWID = m.rowid
WHERE j.posted_at >= date('now', '-1 month')
  -- Broaden location to catch ATS remote inaccuracies before local LLM extracts the truth
  AND (
    j.location LIKE '%Ukraine%' OR
    j.location LIKE '%Europe%' OR
    j.location LIKE '%Remote%' OR
    j.location LIKE '%EMEA%' OR
    j.location LIKE '%Worldwide%' OR
    j.location LIKE '%Global%'
    )
ORDER BY m.rank_score ASC,
         j.posted_at DESC;

SELECT COUNT(id)
FROM jobs;
WHERE is_normalized = 1;

UPDATE jobs
SET company_slug = '3ex'
WHERE id IN (SELECT j.id as counts
             FROM jobs AS j
             WHERE ats_type = 'breezy'
               and company_slug = '3EX');

SELECT j.id
FROM jobs AS j
WHERE j.ats_type = 'breezy'
  and j.company_slug = '3EX';

SELECT a.company_slug, a.company_name, a.url
FROM ats AS a
WHERE company_slug LIKE '%3EX%'
  AND ats_name = 'breezy';

DELETE
FROM ats
WHERE company_slug = 'alamo'
  AND ats_name = 'cornerstone';

INSERT INTO ats (ats_name, company_slug, company_name, url, tier)
VALUES ('tiktok', 'TikTok', 'TikTok', '', 2);

UPDATE ats
SET company_slug = 'alamo'
WHERE ats_name = 'cornerstone'
  AND company_name = 'Alamo';

SELECT j.id, j.company_slug, a.company_slug, a.company_name
FROM jobs AS j
         LEFT JOIN ats AS a ON j.company_slug = a.company_name AND j.ats_type = a.ats_name
WHERE a.company_name IS NULL
  AND j.ats_type = 'breezy';



DELETE
FROM jobs
WHERE ats_type = 'breezy';
;


UPDATE jobs
SET company_slug = ats.company_slug
FROM ats
WHERE id IN ();


SELECT j.id, j.company_slug, a.company_name, a.company_slug
FROM jobs AS j
         LEFT JOIN ats AS a ON j.ats_type = a.ats_name AND j.company_slug = a.company_name
WHERE ats_type = 'adp'
  AND a.company_slug IS NULL;

SELECT DISTINCT company_slug FROM jobs
WHERE ats_type = 'cornerstone';

SELECT COUNT(*) FROM ats
WHERE ats_name = 'dayforce';


DELETE FROM jobs
WHERE ats_type = 'dayforce';
