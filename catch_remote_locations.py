import re
import asyncio
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Initialize the asynchronous SQLite engine using aiosqlite
DATABASE_URL = "sqlite+aiosqlite:///../jobs.db"
engine = create_async_engine(DATABASE_URL, echo=False)

# Configure the async session factory
async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

def evaluate_location_relevance(location_str: str) -> str:
    """
    Analyzes a location string based on a real-world dataset of 3.7M vacancies.
    Rejects raw non-remote geographic data and filters localized exclusions.
    """
    if not location_str:
        return "REJECT_EMPTY"

    # 1. STANDARDIZATION
    location_raw = location_str.strip()
    location = location_raw.lower()

    # ==========================================
    # STEP 1: CONTEXT SECURITY GATE (Eliminate pure On-Site/Hybrid data)
    # ==========================================
    # If the string doesn't say "remote" or "wfh", it's an office job city/hub. Reject it.
    remote_context_tokens = ['remote', 'wfh', 'home', 'anywhere', 'worldwide', 'world wide', 'global', 'emea', 'europe']
    if not any(token in location for token in remote_context_tokens):
        return "REJECT_NO_REMOTE_CONTEXT"

    # Explicitly catch strings that say "hybrid" unless they also match local criteria later
    if 'hybrid' in location and not any(m in location for m in ['ukraine', 'kyiv', 'kiev', 'lviv']):
        return "REJECT_HYBRID_NON_LOCAL"

    # ==========================================
    # STEP 2: HARD EXCLUSIONS (Negative Filters First)
    # ==========================================

    # A. US Specific Phrases & Cities
    us_specific_words = [
        'united states', 'america', 'usa', 'u.s.', 'nationwide', 'any state', 'san francisco',
        'austin', 'boston', 'chicago', 'seattle', 'atlanta', 'new york', 'los angeles', 'silicon valley',
        'denver', 'dallas', 'houston', 'miami', 'philadelphia', 'phoenix', 'portland', 'noram', 'us_remote',
        'columbus', 'leawood'  # Added leaks from your dataset sample
    ]
    if any(w in location for w in us_specific_words) or re.search(r'\b(us|usa|u\.s)\b', location) or location.endswith('-united-states'):
        return "EXCLUDE_US"

    # B. US Case-Sensitive 2-Letter State Codes
    state_codes = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA',
        'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT',
        'VA', 'WA', 'WV', 'WI', 'WY', 'PR', 'DC', 'OH'
    ]
    if re.search(r'\b(' + '|'.join(state_codes) + r')\b', location_raw):
        return "EXCLUDE_US_STATE"

    # C. Full US State Names
    us_states_full = [
        'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware',
        'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky',
        'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi',
        'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico',
        'new york', 'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
        'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont',
        'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming'
    ]
    if any(w in location for w in us_states_full):
        return "EXCLUDE_US_STATE"

    # D. Non-compatible International Hubs & Countries (Including common spelling variations)
    other_countries_and_hubs = [
        'brazil', 'brasil', 'mexico', 'canada', 'india', 'germany', 'deutschland', 'united kingdom', 'london', 'uk', 'gb',
        'spain', 'philippines', 'colombia', 'poland', 'ireland', 'france', 'australia', 'singapore', 'japan',
        'china', 'netherlands', 'sweden', 'switzerland', 'austria', 'belgium', 'denmark', 'norway', 'finland',
        'argentina', 'portugal', 'serbia', 'romania', 'turkey', 'türkiye', 'slovakia', 'hungary', 'costa rica',
        'chile', 'paris', 'ontario', 'toronto', 'vancouver', 'sydney', 'melbourne', 'berlin', 'munich', 'amsterdam',
        'pakistan', 'italy', 'taiwan', 'south korea', 'cyprus', 'bulgaria', 'vietnam', 'malaysia', 'armenia', 'indonesia',
        'bangalore', 'manila', 'münchen', 'hamburg', 'düsseldorf', 'são paulo', 'sao paulo', 'bogota', 'madrid', 'barcelona',
        'south africa', 'dubai', 'lyon', 'bangkok', 'thailand', 'abuja', 'nigeria', 'seoul', 'derbyshire', 'guatemala', 'leeds',
        'sharjah', 'ae', 'boisbriand', 'taupo', 'home counties', 'derbyshire', 'manchester', 'birmingham', 'scotland', 'wales'
    ]
    if any(c in location for c in other_countries_and_hubs):
        return "EXCLUDE_OTHER_COUNTRY"

    # E. ISO 2-letter Country Codes
    country_iso2 = ['br', 'mx', 'ca', 'in', 'de', 'uk', 'es', 'pl', 'fr', 'au', 'sg', 'ie', 'pt', 'tr', 'ro', 'rs', 'pk', 'it', 'tw', 'kr', 'cy', 'bg', 'vn', 'my', 'am', 'id', 'co', 'za', 'lk', 'cn', 'cz', 'th', 'ae', 'ng', 'gt']
    if re.search(r'\b(' + '|'.join(country_iso2) + r')\b', location):
        return "EXCLUDE_OTHER_COUNTRY"

    # F. ISO 3-letter Country Prefixes
    if re.search(r'\b(ind|bra|bgr|arg|deu|aus|can|usa|phl|gbr|fra|esp|mex|col|prt|tha|are|nga|gtm)\b', location):
        return "EXCLUDE_OTHER_COUNTRY"

    # ==========================================
    # STEP 3: INCLUSIONS (Positive Filters Later)
    # ==========================================

    # 1. TARGETED LOCAL REMOTE (Highest Positive Priority)
    ukraine_markers = ['ukraine', 'kyiv', 'kiev', 'lviv', 'odessa', 'kharkiv', 'dnipro']
    if any(w in location for w in ukraine_markers) or re.search(r'\bua\b', location):
        return "KEEP_LOCAL"

    # 2. GLOBAL AND REGIONAL REMOTE
    global_markers = ['worldwide', 'global', 'anywhere', 'world wide', 'emea', 'europe', 'international', 'latam', 'apac', 'utc', 'amer']
    if any(re.search(r'\b' + re.escape(w) + r'\b', location) for w in global_markers) or re.search(r'\b(eu|world)\b', location):
        return "KEEP_GLOBAL"

    # 3. PURE / GENERIC REMOTE
    generic_remotes = {
        'remote', 'remote job', 'remote position', 'remote location', 'any location / remote',
        'remote/homebased', 'remote locations', '1 remote', 'homebased', 'fully remote',
        'remotely based', 'remote office', '100% remote', 'field/remote', 'remote worker - wfh',
        'remote home office', 'remote worker', 'work from home', 'wfh', 'remote; work from home'
    }
    if location in generic_remotes or location.replace(' ', '') == '100%remote':
        return "KEEP_PURE"

    return "POTENTIAL_PURE"

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

        print(f"Beginning processing loop for {len(raw_items)} items...")

        for row in mappings:
            # Safely evaluate and append output dict elements to prevent malformed structures
            status = evaluate_location_relevance(row['location'])
            evaluated_pairs.append({
                "id": row['job_id'],
                "is_remote": status
            })

        if not evaluated_pairs:
            print("No evaluation data generated.")
            return

        # Load processed calculations into evaluation DataFrame
        res_df = pd.DataFrame(evaluated_pairs)

        # Relational JOIN to completely avoid array misalignments or row-shifting bugs
        df = df.merge(res_df, left_on='job_id', right_on='id', how='left')

        # Step 4: Map values to target dict bindings and perform highly optimized asynchronous Bulk Update
        print("Writing calculated 'is_remote' categories back to SQLite via Bulk execution...")

        # Safely convert extracted Series rows containing evaluation outcomes to bind parameters
        update_data = [
            {
                "remote_status": str(row['is_remote']),
                "j_id": int(row['job_id'])
            }
            for _, row in df.dropna(subset=['is_remote']).iterrows()
        ]

        # Triggers SQLAlchemy execution optimization mechanism inside a single database transaction block
        await session.execute(
            text("UPDATE test_golden_set SET is_remote = :remote_status WHERE job_id = :j_id;"),
            update_data
        )

        # Persist calculations structurally to disk
        await session.commit()
        print("Database evaluation complete!")

if __name__ == "__main__":
    asyncio.run(run_matrix_evaluation())