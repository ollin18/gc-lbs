#!/usr/bin/env bash
set -e

# Display help
show_help() {
  echo "Usage: $0 [options] COUNTRY YEAR"
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID (default: $PROJECT)"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket (default: $INPUT_BUCKET)"
  echo "  -o, --output-bucket BUCKET Output GCS bucket (default: $OUTPUT_BUCKET)"
  echo "  -d, --dataset DATASET      BigQuery dataset name (default: $DATASET)"
  echo "  -dt, --distance METERS     Distance threshold in meters (default: $DISTANCE_THRESHOLD)"
  echo "  -tt, --time SECONDS        Time threshold in seconds (default: $TIME_THRESHOLD)"
  echo "  -sd, --stop-duration MS    Minimum stop duration in ms (default: $MIN_STOP_DURATION)"
  echo "  -h, --help                 Show this help message"
  exit 1
}

# Default configuration
# PROJECT="job-accessibility"
# INPUT_BUCKET="lbs-laltam"
# OUTPUT_BUCKET="lbs-laltam"
# DATASET="lbs_latam"
# DISTANCE_THRESHOLD=20          # meters
# TIME_THRESHOLD=3600            # seconds (1 hour)
# MIN_STOP_DURATION=300000       # milliseconds (5 minutes)

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
    -dt|--distance)
      DISTANCE_THRESHOLD="$2"
      shift 2
      ;;
    -tt|--time)
      TIME_THRESHOLD="$2"
      shift 2
      ;;
    -sd|--stop-duration)
      MIN_STOP_DURATION="$2"
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
if [ "$#" -ne 2 ]; then
  echo "Error: COUNTRY and YEAR arguments are required"
  show_help
fi

COUNTRY=$1
YEAR=$2

# Check that required environment variables are set, or fail with a helpful message
: "${PROJECT:?Must set PROJECT}"
: "${INPUT_BUCKET:?Must set INPUT_BUCKET}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
: "${DISTANCE_THRESHOLD:?Must set DISTANCE_THRESHOLD}"
: "${TIME_THRESHOLD:?Must set TIME_THRESHOLD}"
: "${MIN_STOP_DURATION:?Must set MIN_STOP_DURATION}"

echo "Running process_yearly_stays with configuration:"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Country: $COUNTRY"
echo "Year: $YEAR"
echo "Distance threshold: $DISTANCE_THRESHOLD meters"
echo "Time threshold: $TIME_THRESHOLD seconds"
echo "Minimum stop duration: $MIN_STOP_DURATION ms"

# Build table names and paths
TNAME="${COUNTRY}${YEAR}"
TABLE_NAME="${DATASET}.${TNAME}"
SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/${COUNTRY}/${YEAR}/part-*.parquet"

echo "Loading data from $SOURCE_URI to $TABLE_NAME..."
bq load \
  --source_format=PARQUET \
  --autodetect \
  ${TABLE_NAME} \
  ${SOURCE_URI}

# Build the query using a here-document
QUERY=$(cat <<EOF
EXPORT DATA
OPTIONS (
  uri='${EXPORT_URI}',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
)
AS

WITH base AS (
  SELECT DISTINCT
    device_id,
    latitude,
    longitude,
    timestamp
  FROM \`${TABLE}\`
),

ordered_base AS (
  SELECT
    device_id,
    latitude,
    longitude,
    timestamp,
    LEAD(latitude) OVER w AS next_latitude,
    LEAD(longitude) OVER w AS next_longitude,
    LEAD(timestamp) OVER w AS next_timestamp
  FROM base
  WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
),

calc AS (
  SELECT
    device_id,
    latitude,
    longitude,
    timestamp,
    next_latitude,
    next_longitude,
    next_timestamp,
    CASE WHEN next_latitude IS NOT NULL AND next_longitude IS NOT NULL THEN
      ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(next_longitude, next_latitude))
    ELSE NULL END AS distance,
    CASE WHEN next_timestamp IS NOT NULL THEN (next_timestamp - timestamp) / 1000 ELSE NULL END AS time_diff
  FROM ordered_base
),

flags AS (
  SELECT
    device_id,
    latitude,
    longitude,
    timestamp,
    distance,
    time_diff,
    distance <= ${DISTANCE_THRESHOLD} AS within_radius,
    (time_diff <= ${TIME_THRESHOLD} OR time_diff IS NULL) AS within_time,
    (distance <= ${DISTANCE_THRESHOLD} AND (time_diff <= ${TIME_THRESHOLD} OR time_diff IS NULL)) AS stationary
  FROM calc
),

events AS (
  SELECT
    device_id,
    latitude,
    longitude,
    timestamp,
    stationary,
    CASE
      WHEN stationary AND (LAG(stationary) OVER w IS NULL OR LAG(stationary) OVER w = FALSE)
      THEN 1 ELSE 0
    END AS event_start
  FROM flags
  WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
),

cumulative AS (
  SELECT
    device_id,
    latitude,
    longitude,
    timestamp,
    stationary,
    SUM(event_start) OVER w AS event_id
  FROM events
  WINDOW w AS (PARTITION BY device_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

stationary_points AS (
  SELECT
    device_id,
    timestamp,
    latitude,
    longitude,
    event_id
  FROM cumulative
  WHERE stationary = TRUE AND event_id > 0
),

stops AS (
  SELECT
    device_id,
    event_id AS stop_id,
    MIN(timestamp) AS start_timestamp,
    MAX(timestamp) AS end_timestamp,
    APPROX_QUANTILES(latitude, 100)[OFFSET(50)] AS median_latitude,
    APPROX_QUANTILES(longitude, 100)[OFFSET(50)] AS median_longitude
  FROM stationary_points
  GROUP BY device_id, event_id
  HAVING (MAX(timestamp) - MIN(timestamp)) >= ${MIN_STOP_DURATION}  -- Only valid stops > minimum duration
),

deduped_stops AS (
  SELECT DISTINCT *
  FROM stops
),

final_stops AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY start_timestamp) AS stop_events
  FROM deduped_stops
)

SELECT
*
FROM final_stops
ORDER BY device_id, stop_events;
EOF
)

echo "Processing stops..."
bq query --use_legacy_sql=false "$QUERY"

echo "Completed stops processing for ${COUNTRY} ${YEAR}"

