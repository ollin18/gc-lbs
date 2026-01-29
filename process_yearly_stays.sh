#!/usr/bin/env bash
set -e

# Input format defaults (can be overridden by environment)
: "${INPUT_FORMAT:=auto}"
: "${CSV_SEPARATOR:=$'\t'}"
: "${CSV_HEADER:=true}"
: "${COL_DEVICE_ID:=device_id}"
: "${COL_LATITUDE:=latitude}"
: "${COL_LONGITUDE:=longitude}"
: "${COL_TIMESTAMP:=timestamp}"

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
  echo "  --format FORMAT            Input format: auto, parquet, csv (default: auto)"
  echo "  --csv-sep SEP              CSV separator (default: tab)"
  echo "  --no-header                CSV has no header row"
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

# Function to detect input format from GCS
detect_format() {
  local uri="$1"
  local base_path="${uri%/*}"  # Remove the wildcard part

  # List files in the GCS path and check extensions
  local files=$(gsutil ls "${base_path}/" 2>/dev/null | head -20)

  for f in $files; do
    case "$f" in
      *.parquet|*.snappy.parquet)
        echo "parquet"
        return
        ;;
      *.csv|*.csv.gz)
        echo "csv"
        return
        ;;
    esac
  done

  # Default to parquet if no matching files found
  echo "parquet"
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
    --format)
      INPUT_FORMAT="$2"
      shift 2
      ;;
    --csv-sep)
      CSV_SEPARATOR="$2"
      shift 2
      ;;
    --no-header)
      CSV_HEADER=false
      shift
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

# Check vars
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
echo "Input format: $INPUT_FORMAT"
echo "Column mapping: device_id=${COL_DEVICE_ID}, latitude=${COL_LATITUDE}, longitude=${COL_LONGITUDE}, timestamp=${COL_TIMESTAMP}"

# Build paths
TNAME="${COUNTRY}${YEAR}"
TABLE_NAME="${DATASET}.${TNAME}"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/${COUNTRY}/${YEAR}/part-*.parquet"

# Detect format if auto
DETECTED_FORMAT="$INPUT_FORMAT"
if [ "$INPUT_FORMAT" = "auto" ]; then
  # Try to detect format from parquet first, then csv
  PARQUET_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.parquet"
  CSV_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv*"

  if gsutil ls "${PARQUET_URI}" &>/dev/null; then
    DETECTED_FORMAT="parquet"
    SOURCE_URI="$PARQUET_URI"
  elif gsutil ls "${CSV_URI}" &>/dev/null; then
    DETECTED_FORMAT="csv"
    # Check if .csv.gz or .csv
    if gsutil ls "gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv.gz" &>/dev/null; then
      SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv.gz"
    else
      SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv"
    fi
  else
    echo "Error: No parquet or csv files found in gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/"
    exit 1
  fi
  echo "Auto-detected input format: $DETECTED_FORMAT"
elif [ "$INPUT_FORMAT" = "parquet" ]; then
  SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.parquet"
else
  # CSV format - check for compressed or uncompressed
  if gsutil ls "gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv.gz" &>/dev/null; then
    SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv.gz"
  else
    SOURCE_URI="gs://${INPUT_BUCKET}/${COUNTRY}/${YEAR}/*.csv"
  fi
fi

echo "Loading data from $SOURCE_URI to $TABLE_NAME..."

if [ "$DETECTED_FORMAT" = "parquet" ]; then
  bq load \
    --source_format=PARQUET \
    --autodetect \
    ${TABLE_NAME} \
    ${SOURCE_URI}
else
  # CSV loading with configurable options
  SKIP_ROWS=0
  if [ "$CSV_HEADER" = "true" ]; then
    SKIP_ROWS=1
  fi

  # Map CSV separator to BigQuery field delimiter
  FIELD_DELIMITER="$CSV_SEPARATOR"

  bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=${SKIP_ROWS} \
    --field_delimiter="${FIELD_DELIMITER}" \
    --allow_quoted_newlines \
    ${TABLE_NAME} \
    ${SOURCE_URI}
fi

# Check if column renaming is needed (column names differ from standard)
NEEDS_RENAME=false
if [ "$COL_DEVICE_ID" != "device_id" ] || [ "$COL_LATITUDE" != "latitude" ] || \
   [ "$COL_LONGITUDE" != "longitude" ] || [ "$COL_TIMESTAMP" != "timestamp" ]; then
  NEEDS_RENAME=true
fi

#query
# Build column selection based on mapping
if $NEEDS_RENAME; then
  COL_SELECT="${COL_DEVICE_ID} AS device_id, ${COL_LATITUDE} AS latitude, ${COL_LONGITUDE} AS longitude, ${COL_TIMESTAMP} AS timestamp"
else
  COL_SELECT="device_id, latitude, longitude, timestamp"
fi

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
    ${COL_SELECT}
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
    LAG(distance) OVER w AS prev_distance,
    LAG(time_diff) OVER w AS prev_time_diff,
    -- A ping is part of a stay if it's close in time and space to NEXT point OR PREVIOUS point
    (
      (distance <= ${DISTANCE_THRESHOLD} AND (time_diff <= ${TIME_THRESHOLD} OR time_diff IS NULL))
      OR
      (LAG(distance) OVER w <= ${DISTANCE_THRESHOLD} AND (LAG(time_diff) OVER w <= ${TIME_THRESHOLD} OR LAG(time_diff) OVER w IS NULL))
    ) AS stationary
  FROM calc
  WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
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
initial_stops AS (
  SELECT
    device_id,
    event_id AS stop_id,
    MIN(timestamp) AS start_timestamp,
    MAX(timestamp) AS end_timestamp,
    APPROX_QUANTILES(latitude, 100)[OFFSET(50)] AS median_latitude,
    APPROX_QUANTILES(longitude, 100)[OFFSET(50)] AS median_longitude
  FROM stationary_points
  GROUP BY device_id, event_id
  HAVING (MAX(timestamp) - MIN(timestamp)) >= ${MIN_STOP_DURATION}
),
-- Filter points to remove outliers based on distance to median
filtered_points AS (
  SELECT
    sp.device_id,
    sp.timestamp,
    sp.latitude,
    sp.longitude,
    sp.event_id
  FROM stationary_points sp
  JOIN initial_stops ist
    ON sp.device_id = ist.device_id
    AND sp.event_id = ist.stop_id
  WHERE ST_DISTANCE(
    ST_GEOGPOINT(sp.longitude, sp.latitude),
    ST_GEOGPOINT(ist.median_longitude, ist.median_latitude)
  ) <= ${DISTANCE_THRESHOLD}
),
stops AS (
  SELECT
    device_id,
    event_id AS stop_id,
    MIN(timestamp) AS start_timestamp,
    MAX(timestamp) AS end_timestamp,
    APPROX_QUANTILES(latitude, 100)[OFFSET(50)] AS median_latitude,
    APPROX_QUANTILES(longitude, 100)[OFFSET(50)] AS median_longitude
  FROM filtered_points
  GROUP BY device_id, event_id
  HAVING (MAX(timestamp) - MIN(timestamp)) >= ${MIN_STOP_DURATION}  -- check again after filtering
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

