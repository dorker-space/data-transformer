UPDATE jobs 
SET is_remote = CASE 
    -- 1. KEEP_LOCAL (Ukraine)
    WHEN LOWER(location) GLOB '*ukraine*' OR LOWER(location) GLOB '*kyiv*' 
         OR LOWER(location) GLOB '*kiev*' OR LOWER(location) GLOB '*lviv*' 
         OR LOWER(location) GLOB '*odessa*' OR LOWER(location) GLOB '*kharkiv*' 
         OR LOWER(location) GLOB '*dnipro*' OR LOWER(location) LIKE '% ua %' THEN 1

    -- 2. KEEP_GLOBAL / REGIONAL
    WHEN LOWER(location) GLOB '*worldwide*' OR LOWER(location) GLOB '*global*' 
         OR LOWER(location) GLOB '*anywhere*' OR LOWER(location) GLOB '*world wide*' 
         OR LOWER(location) GLOB '*emea*' OR LOWER(location) GLOB '*europe*' 
         OR LOWER(location) GLOB '*international*' OR LOWER(location) GLOB '*latam*' 
         OR LOWER(location) GLOB '*apac*' OR LOWER(location) GLOB '*utc*' 
         OR LOWER(location) GLOB '*amer*' OR LOWER(location) LIKE '% eu %' THEN 1

    -- 3. KEEP_PURE (Exact matches from generic list)
    WHEN LOWER(TRIM(location)) IN (
        'remote', 'remote job', 'remote position', 'remote location', 'any location / remote', 
        'remote/homebased', 'remote locations', '1 remote', 'homebased', 'fully remote', 
        'remotely based', 'remote office', '100% remote', 'field/remote', 'remote worker - wfh', 
        'remote home office', 'remote worker', 'work from home', 'wfh', 'remote; work from home'
    ) OR REPLACE(LOWER(location), ' ', '') = '100%remote' THEN 1

    -- 4. EXCLUDE_US (Hard drops)
    WHEN LOWER(location) GLOB '*united states*' OR LOWER(location) GLOB '*america*' 
         OR LOWER(location) GLOB '*usa*' OR LOWER(location) GLOB '*u.s.*' 
         OR LOWER(location) GLOB '*nationwide*' OR LOWER(location) GLOB '*san francisco*' 
         OR LOWER(location) GLOB '*austin*' OR LOWER(location) GLOB '*boston*' 
         OR LOWER(location) GLOB '*chicago*' OR LOWER(location) GLOB '*seattle*' 
         OR LOWER(location) GLOB '*atlanta*' OR LOWER(location) GLOB '*new york*' 
         OR LOWER(location) GLOB '*los angeles*' OR LOWER(location) GLOB '*silicon valley*' THEN 0

    -- 5. EXCLUDE_OTHER_COUNTRIES
    WHEN LOWER(location) GLOB '*brazil*' OR LOWER(location) GLOB '*mexico*' 
         OR LOWER(location) GLOB '*canada*' OR LOWER(location) GLOB '*india*' 
         OR LOWER(location) GLOB '*germany*' OR LOWER(location) GLOB '*deutschland*' 
         OR LOWER(location) GLOB '*united kingdom*' OR LOWER(location) GLOB '*london*' 
         OR LOWER(location) GLOB '*uk*' OR LOWER(location) GLOB '*gb*' 
         OR LOWER(location) GLOB '*spain*' OR LOWER(location) GLOB '*philippines*' 
         OR LOWER(location) GLOB '*colombia*' OR LOWER(location) GLOB '*poland*' THEN 0

    ELSE 1 
END
WHERE location IS NOT NULL;