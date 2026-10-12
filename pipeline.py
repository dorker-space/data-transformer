import asyncio
import logging
import pathlib
import sys
from datetime import datetime
from typing import Literal, cast
import os

import aiosqlite
import httpx
import polars as pl
from pydantic import BaseModel, Field, ConfigDict, HttpUrl, JsonValue

# If this module is local, ensure it is available in your PYTHONPATH
from evaluate_location_relevance import evaluate_location_relevance

# ===== MODELS =====
EmploymentType = Literal["FULL_TIME", "PART_TIME", "CONTRACT", "INTERN", "TEMPORARY"]
IsRemote = Literal[
    "REJECT_EMPTY", "REJECT_NO_REMOTE_CONTEXT", "KEEP_GLOBAL", "EXCLUDE_US",
    "REJECT_HYBRID_NON_LOCAL", "POTENTIAL_PURE", "EXCLUDE_US_STATE",
    "EXCLUDE_OTHER_COUNTRY", "KEEP_GLOBAL", "KEEP_LOCAL", "KEEP_PURE"
]

class Job(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ats_type: str = Field(..., description="The target applicant tracking system platform.")
    ats_id: str = Field(..., description="The unique, platform-specific identifier.")
    url: HttpUrl = Field(..., description="The direct public career page URL.")
    apply_url: HttpUrl | None = Field(default=None, description="The dedicated endpoint for submitting applications.")
    title: str = Field(..., description="The unformatted, literal job title.")
    company_slug: str = Field(..., description="The normalized identifier of the hiring entity.")
    location: str | None = Field(default=None, description="The free-form raw location string.")
    is_remote: IsRemote = Field(default="POTENTIAL_PURE", description="Remote classification logic result.")
    employment_type: EmploymentType | None = Field(default="FULL_TIME", description="Normalized employment category.")
    description: str = Field(..., description="Clean, plain-text job description.")
    salary_min: float | None = Field(default=None, description="Evaluated lower bound of base compensation.")
    salary_max: float | None = Field(default=None, description="Evaluated upper bound of base compensation.")
    salary_currency: str | None = Field(default=None, description="ISO 4217 currency code.")
    application_questions: list[JsonValue] | None = Field(default=None, description="Structured dictionary of custom inputs.")
    posted_at: datetime | None = Field(default=None, description="Initial publication timestamp.")
    fetched_at: datetime = Field(default_factory=datetime.utcnow, description="System timestamp for ingestion.")

# ===== CONFIGURATION =====
DATABASE_FILE = "/Users/serafym/Developer/dorker.space/jobs.db"
DATASET_URL = "https://storage.stapply.ai/jobhive/v1/all.parquet"
LOCAL_TMP_FILE = pathlib.Path("./all_jobs_temp.parquet")
BATCH_SIZE = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SQL_SCHEMA = """
             CREATE TABLE IF NOT EXISTS jobs (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ats_type TEXT NOT NULL,
                 ats_id TEXT NOT NULL,
                 url TEXT NOT NULL,
                 apply_url TEXT,
                 title TEXT NOT NULL,
                 company_slug TEXT NOT NULL,
                 location TEXT,
                 is_remote TEXT,
                 employment_type TEXT DEFAULT 'FULL_TIME',
                 description TEXT NOT NULL,
                 salary_min REAL,
                 salary_max REAL,
                 salary_currency TEXT,
                 application_questions TEXT,
                 posted_at TEXT,
                 fetched_at TEXT NOT NULL
             );

             CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_ats_composite ON jobs (ats_type, ats_id);
             CREATE INDEX IF NOT EXISTS idx_jobs_search_routing ON jobs (is_remote, employment_type); \
             """

INSERT_QUERY = """
               INSERT INTO jobs (
                   ats_type, ats_id, url, apply_url, title, company_slug, location,
                   employment_type, description, salary_min, salary_max, salary_currency,
                   application_questions, posted_at, fetched_at, is_remote
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ats_type, ats_id) DO NOTHING; \
               """

async def init_sqlite_db():
    logger.info("Initializing SQLite file and applying database schema...")
    async with aiosqlite.connect(DATABASE_FILE) as db:
        await db.executescript(SQL_SCHEMA)
        await db.commit()
    logger.info("Database schema applied successfully.")

async def download_dataset_streamed(url: str, destination: pathlib.Path):
    if destination.exists():
        logger.info(f"Local cache found at {destination}. Skipping download.")
        return

    logger.info(f"Streaming dataset from Cloudflare R2: {url}...")
    destination.parent.mkdir(parents=True, exist_ok=True)

    timeout = httpx.Timeout(60.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("GET", url) as response:
            if response.status_code != 200:
                raise RuntimeError(f"R2 stream failed with status: {response.status_code}")

            with open(destination, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)

    logger.info(f"Dataset stored locally at: {destination}")

def map_row_to_domain(row: dict) -> Job:
    emp_type: EmploymentType = "FULL_TIME"
    raw_emp = row.get("employment_type")
    if raw_emp in ["FULL_TIME", "PART_TIME", "CONTRACT", "INTERN", "TEMPORARY"]:
        emp_type = raw_emp

    posted_at = None
    if row.get("posted_at"):
        try:
            posted_at = datetime.fromisoformat(str(row["posted_at"]).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return Job(
        ats_type=str(row["ats_type"]).strip(),
        ats_id=str(row["ats_id"]).strip(),
        url=row["url"],
        apply_url=row["apply_url"] if row.get("apply_url") else None,
        title=str(row["title"]).strip(),
        company_slug=str(row["company"]).strip(),
        location=str(row["location"]).strip() if row.get("location") else None,
        is_remote=cast(IsRemote, evaluate_location_relevance(str(row["location"]).strip())),
        employment_type=emp_type,
        description=str(row["description"]) if row.get("description") else "",
        salary_min=float(row["salary_min"]) if row.get("salary_min") is not None else None,
        salary_max=float(row["salary_max"]) if row.get("salary_max") is not None else None,
        salary_currency=str(row["salary_currency"]).strip() if row.get("salary_currency") else None,
        application_questions=None,
        posted_at=posted_at
    )

def extract_job_tuple(job: Job) -> tuple:
    return (
        job.ats_type,
        job.ats_id,
        str(job.url),
        str(job.apply_url) if job.apply_url else None,
        job.title,
        job.company_slug,
        job.location,
        job.employment_type,
        job.description,
        job.salary_min,
        job.salary_max,
        job.salary_currency,
        None,
        job.posted_at.isoformat() if job.posted_at else None,
        job.fetched_at.isoformat(),
        job.is_remote
    )

async def execute_batch_ingestion(parquet_path: pathlib.Path):
    logger.info("Opening memory-mapped view of the Parquet file...")
    lazy_engine = pl.scan_parquet(str(parquet_path))

    total_available_rows = lazy_engine.select(pl.len()).collect().item()
    logger.info(f"Total available rows in source file: {total_available_rows:,}")

    target_ingestion_limit = total_available_rows

    async with aiosqlite.connect(DATABASE_FILE) as db:
        await db.execute("PRAGMA journal_mode=WAL;")

        for offset in range(0, target_ingestion_limit, BATCH_SIZE):
            batch_df = lazy_engine.slice(offset, BATCH_SIZE).collect()

            # Map into a dictionary to preserve the structured domain objects for logging
            batch_map = {}
            for row_dict in batch_df.iter_rows(named=True):
                try:
                    domain_model = map_row_to_domain(row_dict)
                    key = (domain_model.ats_type, domain_model.ats_id)
                    batch_map[key] = domain_model
                except Exception as row_error:
                    logger.error(f"Error parsing row at offset {offset}: {row_error}")
                    continue

            if not batch_map:
                continue

            # Generate placeholders to check existing records in a single roundtrip
            keys_list = list(batch_map.keys())
            placeholders = ", ".join(["(?, ?)" for _ in keys_list])
            flattened_keys = [item for key in keys_list for item in key]

            check_query = f"""
                SELECT ats_type, ats_id 
                FROM jobs 
                WHERE (ats_type, ats_id) IN ({placeholders})
            """

            # Identify which items already exist in the database
            existing_keys = set()
            async with db.execute(check_query, flattened_keys) as cursor:
                async for row in cursor:
                    existing_keys.add((row[0], row[1]))

            # Split data into valid inserts and conflict logs
            insert_parameters = []
            for key, job in batch_map.items():
                if key in existing_keys:
                    logger.warning(
                        f"Conflict detected: Job {key[0]}:{key[1]} already exists. SQLite omitted entry."
                    )
                else:
                    insert_parameters.append(extract_job_tuple(job))

            if insert_parameters:
                await db.executemany(INSERT_QUERY, insert_parameters)
                await db.commit()

            processed_count = offset + batch_df.height
            progress_percentage = (processed_count / target_ingestion_limit) * 100

            skipped = len(existing_keys)
            inserted = len(insert_parameters)
            logger.info(
                f"Progress: {processed_count:,}/{target_ingestion_limit:,} ({progress_percentage:.2f}%) "
                f"| Batch: {inserted} inserted, {skipped} conflicts skipped."
            )


async def main():
    await init_sqlite_db()

    start_time = datetime.utcnow()
    try:
        await download_dataset_streamed(DATASET_URL, LOCAL_TMP_FILE)
        await execute_batch_ingestion(LOCAL_TMP_FILE)
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if LOCAL_TMP_FILE.exists():
            logger.info("Cleaning up temporary local Parquet storage...")
            LOCAL_TMP_FILE.unlink()

    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"Done! Pipeline execution completed in {duration:.1f} seconds.")

if __name__ == "__main__":

    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
    asyncio.run(main())