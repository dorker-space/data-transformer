import asyncio
import os
import json
from typing import List, Dict
import pandas as pd
from google import genai
from google.genai import types
from google.genai import errors
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Initialize the asynchronous SQLite engine using aiosqlite
DATABASE_URL = "sqlite+aiosqlite:///../jobs.db"
engine = create_async_engine(DATABASE_URL, echo=False)

# Fetch the API key from environment variables and initialize the official Google GenAI client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

# Configure the async session factory
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# System instruction ensures deterministic evaluation logic and strictly defined outputs
system_instruction = """
I'll send you a JSON list of objects containing 'id' and 'location'. Your task is to strictly evaluate whether each position is globally remote (can apply from Ukraine or within EU) or not.

CRITICAL GEO-FILTERING RULES:
1. Default State: Mark as 0 by default.
2. Positive Trigger: You can ONLY mark as 1 if the location explicitly contains the word "Remote", "Worldwide", "Anywhere", or "Global".
3. Conditional Macro-Regions (Europe / EMEA): 
   You can ONLY mark "Europe" or "EMEA" as 1 IF AND ONLY IF the string explicitly contains the word "Remote" (e.g., "Remote (EMEA)", "Remote / Europe"). 
   If it just says "Europe", "Western Europe", or "EMEA" without the word "Remote" nearby — mark as 0.
4. Negative Trigger (Kill-Switch): If the location specifies ANY European/US city or country (e.g., "Berlin", "Amsterdam", "Germany", "Poland", "London") WITHOUT the word "Remote" in the same string — you MUST mark it as 0. 

DO NOT assume a job is remote just because it is in Europe. No "Remote" keyword in foreign location = 0.
"""

# Pydantic models force Gemini to strictly adhere to the structured JSON schema, avoiding raw text drift
class EvaluatedLocation(BaseModel):
    id: int
    status: int

class EvaluationResult(BaseModel):
    results: List[EvaluatedLocation]

async def ask_gemini_batch(items: List[Dict]) -> List[Dict]:
    """
    Sends a chunk of location records to Gemini.
    Passing data as structured JSON prevents array shifting bugs.
    """
    prompt = f"Evaluate following locations:\n{json.dumps(items)}"

    try:
        # The underlying SDK automatically translates HTTP status codes into Python exceptions
        response = await client.aio.models.generate_content(
            model="gemini-3.1-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                temperature=0.0,
                response_schema=EvaluationResult,
            ),
        )
        return [item.model_dump() for item in response.parsed.results]

    except errors.APIError as e:
        # e.code contains the raw HTTP status code (e.g., 429, 500, 503)
        print(f"\n[API Error] HTTP Status Code: {e.code}")
        print(f"[API Error] Message: {e.message}")

        # Reraise the exception so the orchestrator loop knows it needs to back off
        raise e
    except Exception as e:
        print(f"\n[Unexpected Error] Kind: {type(e).__name__} | Details: {e}")
        raise e

async def run_matrix_evaluation() -> None:
    async with async_session_factory() as session:
        # Step 1: Read the unclassified records from your evaluation subset
        result = await session.execute(text("SELECT job_id, location FROM test_golden_set;"))
        mappings = result.mappings().all()

        if not mappings:
            print("The table 'test_golden_set' is empty.")
            return

        # Load raw data into a Pandas DataFrame for downstream vector/matrix manipulations
        df = pd.DataFrame(mappings)

        # Format records into clean dictionaries matching the LLM expected JSON schema
        # Remap the internal database 'job_id' to a clean 'id' key for the model prompt
        raw_items = [{"id": row["job_id"], "location": row["location"]} for row in mappings]

        evaluated_pairs = []
        batch_size = 10
        i = 0

        # Exponential backoff parameters
        base_delay = 2  # Start with a 2-second sleep on error
        max_delay = 60  # Do not sleep longer than 60 seconds
        factor = 2  # Double the sleep time on consecutive errors

        print(f"Beginning processing loop for {len(raw_items)} items...")

        while i < len(raw_items):
            batch = raw_items[i : i + batch_size]
            try:
                # Attempt to process the current chunk
                batch_result = await ask_gemini_batch(batch)
                evaluated_pairs.extend(batch_result)

                # Success: Move index forward and reset delay parameters
                print(f"Processed: {min(i + batch_size, len(raw_items))}/{len(raw_items)}")
                i += batch_size
                base_delay = 2

            except errors.APIError as e:
                # Handle standard API rate limits (HTTP 429) or transient server issues (HTTP 5xx)
                if e.code == 429:
                    print(
                        f"Rate limit hit (429). Backing off for {base_delay} seconds before retry..."
                    )
                else:
                    print(
                        f"Server error ({e.code}). Retrying chunk in {base_delay} seconds..."
                    )

                await asyncio.sleep(base_delay)
                # Dynamically increase sleep time exponentially for the next attempt
                base_delay = min(base_delay * factor, max_delay)

            except Exception:
                # Catch fatal structural/network exceptions to prevent an infinite loop crashing the app silently
                print("Fatal error encountered during batch processing. Aborting.")
                break

        if not evaluated_pairs:
            print("No evaluation data retrieved from the LLM pipeline.")
            return

        # Step 3: Parse LLM outputs into a separate evaluation DataFrame
        res_df = pd.DataFrame(evaluated_pairs)

        # KEY ARCHITECTURAL SAFEGUARD: Merge dataframes via a relational JOIN on IDs.
        # Even if Gemini skips an item or returns them out of order, this completely prevents structural row shifting.
        df = df.merge(res_df, left_on='job_id', right_on='id', how='left')

        # Step 4: Execute an optimized Asynchronous Bulk Update back into SQLite
        print("Writing ground_truth evaluations back to SQLite via Bulk execution...")

        # Map dataframe columns to a structured dictionary parameter list.
        # Dict keys MUST match the bound parameter markers (:g_truth and :j_id) in the raw SQL string.
        update_data = [
            {"g_truth": int(row['status']), "j_id": int(row['job_id'])}
            for _, row in df.dropna(subset=['status']).iterrows()
        ]

        # Passing a list of dictionaries to session.execute triggers SQLAlchemy's executemany mechanism.
        # Instead of 1,000 slow disk I/O mutations, SQLite executes this inside a single atomic transaction block.
        await session.execute(
            text("UPDATE test_golden_set SET ground_truth = :g_truth WHERE job_id = :j_id;"),
            update_data
        )

        # Commit changes safely to disk
        await session.commit()
        print("Database bulk update executed successfully!")

if __name__ == "__main__":
    asyncio.run(run_matrix_evaluation())