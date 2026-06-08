# run_warm_start.py
import asyncio
import logging
import pathlib
import sys
from datetime import datetime

import httpx
import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.domain.model.job import EmploymentType, Job
from src.infra.db.sql_lite.job_repository import JobRepository

DATABASE_URL = "sqlite+aiosqlite:///./jobs.db"

engine = create_async_engine(DATABASE_URL, echo=False)

async_session_factory = async_sessionmaker(
    bind=engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATASET_URL = "https://storage.stapply.ai/jobhive/v1/all.parquet"
LOCAL_TMP_FILE = pathlib.Path("./all_jobs_temp.parquet")
ROW_LIMIT = 1_500_000
BATCH_SIZE = 1000

# Твоя чиста SQL-схема для міграції
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
    is_remote INTEGER DEFAULT 0 NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_jobs_search_routing ON jobs (is_remote, employment_type);
"""

async def init_sqlite_db():
    """Накочує SQL-схему та створює таблиці з індексами, якщо їх немає."""
    logger.info("Initializing SQLite file and applying database schema...")
    async with engine.begin() as conn:
        # Розбиваємо схему на окремі команди, бо SQLite не завжди варить мульти-стейтменти через execute
        for statement in SQL_SCHEMA.split(";"):
            cleaned = statement.strip()
            if cleaned:
                await conn.execute(text(cleaned))
    logger.info("Database schema applied successfully.")

async def download_dataset_streamed(url: str, destination: pathlib.Path):
    """Скачує Parquet-файл з R2 чанками, щоб не забивати RAM."""
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
    """Мапить сирий рядок з Polars у Pydantic контракт Job."""
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
        is_remote=bool(row["is_remote"]) if row.get("is_remote") is not None else False,
        employment_type=emp_type,
        description=str(row["description"]) if row.get("description") else "",
        salary_min=float(row["salary_min"]) if row.get("salary_min") is not None else None,
        salary_max=float(row["salary_max"]) if row.get("salary_max") is not None else None,
        salary_currency=str(row["salary_currency"]).strip() if row.get("salary_currency") else None,
        application_questions=None,
        posted_at=posted_at
    )

async def execute_batch_ingestion(parquet_path: pathlib.Path):
    """Читає локальний Parquet через LazyFrame та пише в SQLite пачками по 1000 рядків."""
    logger.info("Opening memory-mapped view of the Parquet file...")
    lazy_engine = pl.scan_parquet(str(parquet_path))
    
    total_available_rows = lazy_engine.select(pl.len()).collect().item()
    logger.info(f"Total available rows in source file: {total_available_rows:,}")
    
    target_ingestion_limit = total_available_rows
    logger.info(f"Target execution window capped at: {target_ingestion_limit:,} rows.")

    for offset in range(0, target_ingestion_limit, BATCH_SIZE):
        # Матеріалізуємо в пам'ять РІВНО 1000 рядків для поточної пачки
        batch_df = lazy_engine.slice(offset, BATCH_SIZE).collect()
        
        async with async_session_factory() as session:
            repo = JobRepository(session)
            
            for row_dict in batch_df.iter_rows(named=True):
                try:
                    domain_model = map_row_to_domain(row_dict)
                    await repo.save_compact_job(domain_model)
                except Exception as row_error:
                    logger.error(f"Error parsing row at offset {offset}: {row_error}")
                    continue
                    
            # Робимо один єдиний commit на 1000 записів (це рятує швидкість SQLite)
            await session.commit()
            
        processed_count = offset + batch_df.height
        progress_percentage = (processed_count / target_ingestion_limit) * 100
        logger.info(f"Progress: {processed_count:,}/{target_ingestion_limit:,} ({progress_percentage:.2f}%)")

async def main():
    # Крок 1: Ініціалізуємо сам SQLite файл і таблиці через твою схему
    await init_sqlite_db()
    
    start_time = datetime.utcnow()
    try:
        # Крок 2: Качаємо файл чанками
        await download_dataset_streamed(DATASET_URL, LOCAL_TMP_FILE)
        # Крок 3: Заганяємо дані в базу
        await execute_batch_ingestion(LOCAL_TMP_FILE)
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if LOCAL_TMP_FILE.exists():
            logger.info("Cleaning up temporary local Parquet storage...")
            LOCAL_TMP_FILE.unlink()
            
    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"Done! 1.5M jobs synced into local SQLite within {duration:.1f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())