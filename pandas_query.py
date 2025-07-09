import pandas_gbq as pd_gbq

query = """
WITH
-- Get home locations
home_locations AS (
  SELECT DISTINCT
    uid,
    year,
    cluster_latitude AS home_latitude,
    cluster_longitude AS home_longitude
  FROM `job-accessibility.lbs_latam.CO_HW`
  WHERE location_type = 'H'
),

-- Get work locations
work_locations AS (
  SELECT DISTINCT
    uid,
    year,
    cluster_latitude AS work_latitude,
    cluster_longitude AS work_longitude
  FROM `job-accessibility.lbs_latam.CO_HW`
  WHERE location_type = 'W'
)

-- -- Join home and work locations
SELECT
  h.uid,
  h.home_latitude,
  h.home_longitude,
  w.work_latitude,
  w.work_longitude,
  h.year
FROM home_locations h
LEFT JOIN work_locations w
  ON h.uid = w.uid AND h.year = w.year
-- Only return users with a home location
WHERE h.uid IS NOT NULL AND h.home_latitude IS NOT NULL AND w.work_latitude IS
NOT NULL
ORDER BY h.uid, h.year
LIMIT 1000
"""

df = pd_gbq.read_gbq(query, project_id="job-accessibility", dialect="standard")
list(df.columns)

