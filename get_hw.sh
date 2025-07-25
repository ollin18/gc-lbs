#!/usr/bin/env bash
set -e

# Display help
show_help() {
  echo "Usage: $0 [options] COUNTRY"
  echo "Options:"
  echo "  -p, --project PROJECT           Google Cloud project ID (default: $PROJECT)"
  echo "  -i, --input-bucket BUCKET       Input GCS bucket (default: $INPUT_BUCKET)"
  echo "  -o, --output-bucket BUCKET      Output GCS bucket (default: $OUTPUT_BUCKET)"
  echo "  -d, --dataset DATASET           BigQuery dataset name (default: $DATASET)"
  echo "  -hd, --home-days DAYS           Minimum unique days for home (default: $HOME_MIN_DAYS)"
  echo "  -he, --home-evening HOUR        Hour when evening starts (default: $HOME_HOUR_EVENING)"
  echo "  -hm, --home-morning HOUR        Hour when morning ends (default: $HOME_HOUR_MORNING)"
  echo "  -wd, --work-days DAYS           Minimum unique days for work (default: $WORK_MIN_DAYS)"
  echo "  -ws, --work-start HOUR          Work day start hour (default: $WORK_HOUR_START)"
  echo "  -we, --work-end HOUR            Work day end hour (default: $WORK_HOUR_END)"
  echo "  -md, --min-distance METERS      Min distance between home and work (default: $MIN_DISTANCE_HOME_WORK)"
  echo "  -h, --help                      Show this help message"
  exit 1
}

# Parse command line options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project)
      PROJECT="$2"
      shift 2
      ;;
    -i|--input-bucket)
      INPUT_BUCKET="$2"
      shift 2
      ;;
    -o|--output-bucket)
      OUTPUT_BUCKET="$2"
      shift 2
      ;;
    -d|--dataset)
      DATASET="$2"
      shift 2
      ;;
    -hd|--home-days)
      HOME_MIN_DAYS="$2"
      shift 2
      ;;
    -he|--home-evening)
      HOME_HOUR_EVENING="$2"
      shift 2
      ;;
    -hm|--home-morning)
      HOME_HOUR_MORNING="$2"
      shift 2
      ;;
    -wd|--work-days)
      WORK_MIN_DAYS="$2"
      shift 2
      ;;
    -ws|--work-start)
      WORK_HOUR_START="$2"
      shift 2
      ;;
    -we|--work-end)
      WORK_HOUR_END="$2"
      shift 2
      ;;
    -md|--min-distance)
      MIN_DISTANCE_HOME_WORK="$2"
      shift 2
      ;;
    -h|--help)
      show_help
      ;;
    *)
      break
      ;;
  esac
done

# Check if required arguments are provided
if [ "$#" -ne 1 ]; then
  echo "Error: COUNTRY argument is required"
  show_help
fi

COUNTRY=$1

# Check required config variables (from environment file when running from run_pipeline.sh)
: "${PROJECT:?Must set PROJECT}"
: "${INPUT_BUCKET:?Must set INPUT_BUCKET}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"

: "${HOME_MIN_DAYS:?Must set HOME_MIN_DAYS}"
: "${HOME_HOUR_EVENING:?Must set HOME_HOUR_EVENING}"
: "${HOME_HOUR_MORNING:?Must set HOME_HOUR_MORNING}"

: "${WORK_MIN_DAYS:?Must set WORK_MIN_DAYS}"
: "${WORK_HOUR_START:?Must set WORK_HOUR_START}"
: "${WORK_HOUR_END:?Must set WORK_HOUR_END}"

: "${MIN_DISTANCE_HOME_WORK:?Must set MIN_DISTANCE_HOME_WORK}"

echo "Running Home/Work Classification with configuration:"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Country: $COUNTRY"
echo "Home Parameters:"
echo "  - Minimum days: $HOME_MIN_DAYS"
echo "  - Evening hour: $HOME_HOUR_EVENING"
echo "  - Morning hour: $HOME_HOUR_MORNING"
echo "Work Parameters:"
echo "  - Minimum days: $WORK_MIN_DAYS"
echo "  - Work start: $WORK_HOUR_START"
echo "  - Work end: $WORK_HOUR_END"
echo "  - Min distance from home: $MIN_DISTANCE_HOME_WORK meters"

# Build paths
TNAME="${COUNTRY}_local"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/HW/${COUNTRY}/part-*.parquet"

# query
QUERY=$(cat <<EOF
EXPORT DATA
OPTIONS (
  uri='${EXPORT_URI}',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
)
AS

WITH
-- Base table with all data
base AS (
  SELECT *
  FROM \`${TABLE}\`
  WHERE cluster_label >= 0  -- Only consider actual clusters (excluding singletons)
),

-- Count unique days each cluster is visited
cluster_day_counts AS (
  SELECT
    uid,
    year,
    cluster_label,
    COUNT(DISTINCT date) AS unique_days_visited
  FROM base
  GROUP BY uid, year, cluster_label
),

-- Identify potential home locations based on visits during home hours
-- Home hours: weekends + weekdays between evening and morning
home_candidates AS (
  SELECT
    uid,
    year,
    cluster_label,
    cluster_latitude,
    cluster_longitude,
    COUNT(*) AS home_time_visits,
    -- Also count the number of unique days visited during home hours
    COUNT(DISTINCT date) AS home_time_unique_days
  FROM base
  WHERE
    (weekend = TRUE) OR  -- Weekends any time
    (weekend = FALSE AND (hour >= ${HOME_HOUR_EVENING} OR hour < ${HOME_HOUR_MORNING}))  -- Weekdays during home hours
  GROUP BY uid, year, cluster_label, cluster_latitude, cluster_longitude
),

-- Identify home locations with highest home-time visits that meet minimum unique days threshold
home_locations AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (
      PARTITION BY h.uid, h.year
      ORDER BY h.home_time_visits DESC, h.home_time_unique_days DESC, h.cluster_label
    ) AS home_rank,
    dc.unique_days_visited
  FROM home_candidates h
  JOIN cluster_day_counts dc
    ON h.uid = dc.uid AND h.year = dc.year AND h.cluster_label = dc.cluster_label
  WHERE dc.unique_days_visited >= ${HOME_MIN_DAYS}  -- Must be visited at least N different days
),

-- Only keep the top ranked home location for each uid/year
final_home_locations AS (
  SELECT
    uid,
    year,
    cluster_label AS home_cluster_label,
    cluster_latitude AS home_latitude,
    cluster_longitude AS home_longitude,
    home_time_visits,
    unique_days_visited
  FROM home_locations
  WHERE home_rank = 1
),

-- Identify potential work locations based on visits during work hours
-- Work hours: weekdays between work_start and work_end
work_candidates AS (
  SELECT
    uid,
    year,
    cluster_label,
    cluster_latitude,
    cluster_longitude,
    COUNT(*) AS work_time_visits,
    -- Also count the number of unique days visited during work hours
    COUNT(DISTINCT date) AS work_time_unique_days
  FROM base
  WHERE
    weekend = FALSE AND hour >= ${WORK_HOUR_START} AND hour <= ${WORK_HOUR_END}  -- Weekdays during work hours
  GROUP BY uid, year, cluster_label, cluster_latitude, cluster_longitude
),

-- Join with home locations to exclude home clusters
work_candidates_filtered AS (
  SELECT
    w.*,
    h.home_cluster_label,
    -- Calculate distance between this cluster and home cluster (in meters)
    ST_DISTANCE(
      ST_GEOGPOINT(w.cluster_longitude, w.cluster_latitude),
      ST_GEOGPOINT(h.home_longitude, h.home_latitude)
    ) AS distance_from_home,
    CASE
      WHEN h.home_cluster_label IS NULL THEN FALSE  -- No home location identified
      WHEN w.cluster_label = h.home_cluster_label THEN FALSE  -- This is the home cluster
      WHEN w.work_time_unique_days < ${WORK_MIN_DAYS} THEN FALSE  -- Not enough visits during work hours
      ELSE TRUE
    END AS is_valid_work_candidate
  FROM work_candidates w
  LEFT JOIN final_home_locations h
    ON w.uid = h.uid AND w.year = h.year
),

-- Identify work locations with highest work-time visits that meet criteria
work_locations AS (
  SELECT
    uid,
    year,
    cluster_label AS work_cluster_label,
    cluster_latitude AS work_latitude,
    cluster_longitude AS work_longitude,
    work_time_visits,
    ROW_NUMBER() OVER (
      PARTITION BY uid, year
      ORDER BY work_time_visits DESC, work_time_unique_days DESC, cluster_label
    ) AS work_rank
  FROM work_candidates_filtered
  WHERE
    is_valid_work_candidate = TRUE AND
    distance_from_home >= ${MIN_DISTANCE_HOME_WORK}
),

-- Only keep the top ranked work location for each uid/year
final_work_locations AS (
  SELECT
    uid,
    year,
    work_cluster_label,
    work_latitude,
    work_longitude,
    work_time_visits
  FROM work_locations
  WHERE work_rank = 1
),

-- Assign location type labels and compute location change labels
location_type_assignment AS (
  SELECT
    b.*,
    CASE
      WHEN h.home_cluster_label IS NOT NULL AND b.cluster_label = h.home_cluster_label THEN 'H'
      WHEN w.work_cluster_label IS NOT NULL AND b.cluster_label = w.work_cluster_label THEN 'W'
      ELSE 'O'
    END AS location_type
  FROM base b
  LEFT JOIN final_home_locations h
    ON b.uid = h.uid AND b.year = h.year AND b.cluster_label = h.home_cluster_label
  LEFT JOIN final_work_locations w
    ON b.uid = w.uid AND b.year = w.year AND b.cluster_label = w.work_cluster_label
),

-- Calculate location labels (1, 2, 3, etc.) based on changes over years
location_changes AS (
  SELECT
    uid,
    location_type,
    cluster_label,
    year,
    -- For home locations
    CASE WHEN location_type = 'H' THEN
      DENSE_RANK() OVER (
        PARTITION BY uid, location_type
        ORDER BY year, cluster_label
      )
      ELSE -1
    END AS home_change_rank,
    -- For work locations
    CASE WHEN location_type = 'W' THEN
      DENSE_RANK() OVER (
        PARTITION BY uid, location_type
        ORDER BY year, cluster_label
      )
      ELSE -1
    END AS work_change_rank
  FROM location_type_assignment
  GROUP BY uid, location_type, cluster_label, year
),

-- Final combined result
final_result AS (
  SELECT
    t.*,
    CASE
      WHEN t.location_type = 'H' THEN c.home_change_rank
      WHEN t.location_type = 'W' THEN c.work_change_rank
      ELSE -1
    END AS location_label
  FROM location_type_assignment t
  LEFT JOIN location_changes c
    ON t.uid = c.uid
    AND t.location_type = c.location_type
    AND t.cluster_label = c.cluster_label
    AND t.year = c.year
)

-- Select all original columns plus the new ones
SELECT
  uid,
  stop_event,
  start_timestamp,
  end_timestamp,
  stop_duration,
  stop_latitude,
  stop_longitude,
  cluster_label,
  cluster_counts,
  cluster_latitude,
  cluster_longitude,
  timezone,
  inputed_timezone,
  stop_datetime,
  end_stop_datetime,
  date,
  year,
  month,
  day,
  hour,
  weekday,
  weekend,
  end_date,
  location_type,
  location_label
FROM final_result
ORDER BY uid, year, date, start_timestamp;
EOF
)

echo "Running home/work classification query..."
bq query --use_legacy_sql=false "$QUERY"

# Create the final HW table
HW_TNAME="${COUNTRY}_HW"
HW_TABLE_NAME="${DATASET}.${HW_TNAME}"
HW_URI="gs://${OUTPUT_BUCKET}/stops/HW/${COUNTRY}/*.parquet"

echo "Loading home/work data from $HW_URI to $HW_TABLE_NAME..."
bq load \
  --source_format=PARQUET \
  ${HW_TABLE_NAME} \
  ${HW_URI}

echo "Completed home/work classification for ${COUNTRY}"
