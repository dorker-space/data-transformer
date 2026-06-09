from evaluate_location_relevance import evaluate_location_relevance
import asyncio
from typing import List, Dict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Initialize the asynchronous SQLite engine using aiosqlite
DATABASE_URL = "sqlite+aiosqlite:///../jobs.db"
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
)

# Configure the async session factory
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def flush_batch_to_db(session: AsyncSession, batch_data: List[Dict]) -> None:
    """
    Executes high-speed bulk atomic write operations onto disk.
    """
    if not batch_data:
        return

    # Executes the update within SQLAlchemy's bulk parameter binding subsystem
    await session.execute(
        text("UPDATE jobs SET is_remote = :remote_status WHERE id = :j_id;"),
        batch_data
    )
    # Commit changes on a chunk level to clear transactional memory allocation tables
    await session.commit()


async def stream_and_process_jobs() -> None:
    # Tuned block parameters for 3.7 million records execution optimization
    CHUNK_SIZE = 25000  # Number of rows to read AND write per iteration

    print("Configuring explicit ID-pagination pipeline...")

    current_id = 0
    total_processed = 0
    while True:
        # Step 1: Fetch a precise block of data using indexed primary key filtering
        async with async_session_factory() as read_session:
            query = text("""
                         SELECT id, location FROM jobs
                         WHERE id > :current_id
                         ORDER BY id ASC
                             LIMIT :chunk_size;
                         """)
            result = await read_session.execute(query, {"current_id": current_id, "chunk_size": CHUNK_SIZE})
            rows = result.mappings().all()

        # Break the loop if no more rows are returned (end of database reached)
        if not rows:
            break

        write_buffer = []

        # Step 2: Evaluate the current batch in memory
        for row in rows:
            status = evaluate_location_relevance(row['location'])
            write_buffer.append({
                "remote_status": status,
                "j_id": row['id']
            })
            current_id = row['id'] # Track highest ID to shift our pagination window forward

        # Step 3: Flush the evaluated batch immediately to disk inside an atomic transaction
        if write_buffer:
            async with async_session_factory() as write_session:
                await write_session.execute(
                    text("UPDATE jobs SET is_remote = :remote_status WHERE id = :j_id;"),
                    write_buffer
                )
                await write_session.commit()

            total_processed += len(write_buffer)
            print(f"Processed and flushed: {total_processed} records. Current ID position: {current_id}")

    print(f"Pipeline complete! Successfully processed {total_processed} rows.")

if __name__ == "__main__":
    asyncio.run(stream_and_process_jobs())