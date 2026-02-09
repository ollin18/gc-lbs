#!/usr/bin/env bash
set -e

: "${INPUT_FORMAT:=auto}"
: "${CSV_SEPARATOR:=$'\t'}"
: "${CSV_HEADER:=true}"
: "${COL_DEVICE_ID:=device_id}"
: "${COL_LATITUDE:=latitude}"
: "${COL_LONGITUDE:=longitude}"
: "${COL_TIMESTAMP:=timestamp}"
: "${INPUT_PATH:=}"

show_help() {
  echo "Usage: $0 [options] LABEL"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="$2"; shift 2 ;;
    -i|--input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    -o|--output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    -d|--dataset) DATASET="$2"; shift 2 ;;
    -h|--help) show_help ;;
    *) break ;;
  esac
done

[ "$#" -ne 1 ] && show_help
LABEL=$1

: "${PROJECT:?Must set PROJECT}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
: "${DISTANCE_THRESHOLD:?Must set DISTANCE_THRESHOLD}"
: "${TIME_THRESHOLD:?Must set TIME_THRESHOLD}"
: "${MIN_STOP_DURATION:?Must set MIN_STOP_DURATION}"

TABLE_NAME="${DATASET}.${LABEL}_raw"
TABLE="${PROJECT}.${DATASET}.${LABEL}_raw"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/${LABEL}/part-*.parquet"

# Determine input path
if [ -n "$INPUT_PATH" ]; then
  BASE_PATH="$INPUT_PATH"
else
  : "${INPUT_BUCKET:?Must set INPUT_BUCKET or INPUT_PATH}"
  BASE_PATH="gs://${INPUT_BUCKET}/${LABEL}"
fi

echo "Running stops detection with configuration:"
echo "  Project: $PROJECT"
echo "  Dataset: $DATASET"
echo "  Label: $LABEL"
echo "  Distance threshold: ${DISTANCE_THRESHOLD} meters"
echo "  Time threshold: ${TIME_THRESHOLD} seconds"
echo "  Min stop duration: ${MIN_STOP_DURATION} seconds"
echo "  Input format: $INPUT_FORMAT"
echo "  Column mapping: device_id=${COL_DEVICE_ID}, latitude=${COL_LATITUDE}, longitude=${COL_LONGITUDE}, timestamp=${COL_TIMESTAMP}"
[ -n "$INPUT_PATH" ] && echo "  Custom input path: $INPUT_PATH"
echo ""

# Detect format (check recursively for nested directories)
DETECTED_FORMAT="$INPUT_FORMAT"
if [ "$INPUT_FORMAT" = "auto" ]; then
  if gsutil ls "${BASE_PATH}/**/*.parquet" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="parquet"
  elif gsutil ls "${BASE_PATH}/*.parquet" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="parquet"
  elif gsutil ls "${BASE_PATH}/**/*.csv.gz" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="csv"
  elif gsutil ls "${BASE_PATH}/*.csv.gz" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="csv"
  elif gsutil ls "${BASE_PATH}/**/*.csv" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="csv"
  elif gsutil ls "${BASE_PATH}/*.csv" 2>/dev/null | head -1 | grep -q .; then
    DETECTED_FORMAT="csv"
  else
    echo "Error: No parquet or csv files found in ${BASE_PATH}/ (searched recursively)"
    exit 1
  fi
  echo "Auto-detected input format: $DETECTED_FORMAT"
fi

# Find all files (bq load doesn't support ** so we list and pass as comma-separated URIs)
echo "Finding input files..."
if [ "$DETECTED_FORMAT" = "parquet" ]; then
  SOURCE_FILES=$(gsutil ls "${BASE_PATH}/**/*.parquet" 2>/dev/null || gsutil ls "${BASE_PATH}/*.parquet" 2>/dev/null)
else
  SOURCE_FILES=$(gsutil ls "${BASE_PATH}/**/*.csv.gz" 2>/dev/null || \
                 gsutil ls "${BASE_PATH}/*.csv.gz" 2>/dev/null || \
                 gsutil ls "${BASE_PATH}/**/*.csv" 2>/dev/null || \
                 gsutil ls "${BASE_PATH}/*.csv" 2>/dev/null)
fi

[ -z "$SOURCE_FILES" ] && { echo "Error: No ${DETECTED_FORMAT} files found"; exit 1; }

FILE_COUNT=$(echo "$SOURCE_FILES" | wc -l)
echo "Found $FILE_COUNT ${DETECTED_FORMAT} files"

# Get unique directories (for batch loading since bq has arg length limits)
DIRS=$(echo "$SOURCE_FILES" | xargs -n1 dirname | sort -u)
DIR_COUNT=$(echo "$DIRS" | wc -l)

echo ""
echo "Loading data from ${BASE_PATH} to $TABLE_NAME..."
echo "Loading from $DIR_COUNT directories..."

if [ "$DETECTED_FORMAT" = "parquet" ]; then
  EXT="parquet"
else
  # Determine extension
  if echo "$SOURCE_FILES" | head -1 | grep -q '\.csv\.gz$'; then
    EXT="csv.gz"
  else
    EXT="csv"
  fi

  SKIP_ROWS=0
  [ "$CSV_HEADER" = "true" ] && SKIP_ROWS=1

  # Read header from first file to build schema with all STRING types
  FIRST_FILE=$(echo "$SOURCE_FILES" | head -1)
  echo "Reading header from $FIRST_FILE..."

  if [[ "$FIRST_FILE" == *.gz ]]; then
    HEADER=$(gsutil cat "$FIRST_FILE" | gunzip | head -1)
  else
    HEADER=$(gsutil cat "$FIRST_FILE" | head -1)
  fi

  # Build schema with all columns as STRING
  SCHEMA=""
  IFS="$CSV_SEPARATOR" read -ra COLUMNS <<< "$HEADER"
  for col in "${COLUMNS[@]}"; do
    col=$(echo "$col" | tr -d '"' | tr -d "'" | xargs)
    [ -n "$SCHEMA" ] && SCHEMA="${SCHEMA},"
    SCHEMA="${SCHEMA}${col}:STRING"
  done
  echo "Detected ${#COLUMNS[@]} columns, loading all as STRING..."
fi

# Load by directory (BigQuery has command length limits)
# First directory creates the table, rest append in parallel
MAX_PARALLEL=${MAX_PARALLEL:-10}  # Max parallel jobs

FIRST_DIR=$(echo "$DIRS" | head -1)
REST_DIRS=$(echo "$DIRS" | tail -n +2)

echo "Creating table with first directory..."
URI="${FIRST_DIR}/*.${EXT}"
if [ "$DETECTED_FORMAT" = "parquet" ]; then
  bq load --source_format=PARQUET --autodetect --replace ${TABLE_NAME} "${URI}"
else
  bq load --source_format=CSV --skip_leading_rows=${SKIP_ROWS} --field_delimiter="${CSV_SEPARATOR}" \
    --allow_quoted_newlines --replace ${TABLE_NAME} "${URI}" "${SCHEMA}"
fi

# Append remaining directories in parallel
if [ -n "$REST_DIRS" ]; then
  REST_COUNT=$(echo "$REST_DIRS" | wc -l)
  echo "Appending $REST_COUNT directories in parallel (max $MAX_PARALLEL concurrent)..."

  RUNNING=0
  for dir in $REST_DIRS; do
    URI="${dir}/*.${EXT}"
    (
      if [ "$DETECTED_FORMAT" = "parquet" ]; then
        bq load --source_format=PARQUET --autodetect --noreplace ${TABLE_NAME} "${URI}" 2>&1
      else
        bq load --source_format=CSV --skip_leading_rows=${SKIP_ROWS} --field_delimiter="${CSV_SEPARATOR}" \
          --allow_quoted_newlines --noreplace ${TABLE_NAME} "${URI}" "${SCHEMA}" 2>&1
      fi
    ) &

    RUNNING=$((RUNNING + 1))
    if [ $RUNNING -ge $MAX_PARALLEL ]; then
      wait -n  # Wait for any one job to finish
      RUNNING=$((RUNNING - 1))
    fi
  done

  echo "Waiting for remaining jobs to complete..."
  wait  # Wait for all remaining jobs
fi

echo "All directories loaded."

# Build column select with casting
if [ "$COL_DEVICE_ID" != "device_id" ] || [ "$COL_LATITUDE" != "latitude" ] || \
   [ "$COL_LONGITUDE" != "longitude" ] || [ "$COL_TIMESTAMP" != "timestamp" ]; then
  COL_SELECT="CAST(${COL_DEVICE_ID} AS STRING) AS device_id, CAST(${COL_LATITUDE} AS FLOAT64) AS latitude, CAST(${COL_LONGITUDE} AS FLOAT64) AS longitude, CAST(${COL_TIMESTAMP} AS INT64) AS timestamp"
else
  COL_SELECT="CAST(device_id AS STRING) AS device_id, CAST(latitude AS FLOAT64) AS latitude, CAST(longitude AS FLOAT64) AS longitude, CAST(timestamp AS INT64) AS timestamp"
fi

QUERY=$(cat <<EOF
EXPORT DATA OPTIONS (uri='${EXPORT_URI}', format='PARQUET', compression='SNAPPY', overwrite=true) AS
WITH base AS (
  SELECT DISTINCT ${COL_SELECT} FROM \`${TABLE}\`
),
ordered AS (
  SELECT *, LEAD(latitude) OVER w AS next_lat, LEAD(longitude) OVER w AS next_lon, LEAD(timestamp) OVER w AS next_ts
  FROM base WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
),
calc AS (
  SELECT *,
    CASE WHEN next_lat IS NOT NULL THEN ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(next_lon, next_lat)) END AS dist,
    CASE WHEN next_ts IS NOT NULL THEN (next_ts - timestamp) END AS tdiff
  FROM ordered
),
flags AS (
  -- A ping is part of a stay if it's close in time and space to NEXT point OR PREVIOUS point
  SELECT *,
    (
      (dist <= ${DISTANCE_THRESHOLD} AND (tdiff <= ${TIME_THRESHOLD} OR tdiff IS NULL))
      OR
      (LAG(dist) OVER w <= ${DISTANCE_THRESHOLD} AND (LAG(tdiff) OVER w <= ${TIME_THRESHOLD} OR LAG(tdiff) OVER w IS NULL))
    ) AS stationary
  FROM calc
  WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
),
events AS (
  SELECT *, CASE WHEN stationary AND (LAG(stationary) OVER w IS NULL OR NOT LAG(stationary) OVER w) THEN 1 ELSE 0 END AS ev_start
  FROM flags WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
),
cumul AS (
  SELECT *, SUM(ev_start) OVER (PARTITION BY device_id ORDER BY timestamp ROWS UNBOUNDED PRECEDING) AS ev_id
  FROM events
),
stationary_points AS (
  SELECT device_id, timestamp, latitude, longitude, ev_id
  FROM cumul WHERE stationary AND ev_id > 0
),
initial_stops AS (
  SELECT device_id, ev_id AS stop_id, MIN(timestamp) AS start_timestamp, MAX(timestamp) AS end_timestamp,
    APPROX_QUANTILES(latitude, 100)[OFFSET(50)] AS median_latitude, APPROX_QUANTILES(longitude, 100)[OFFSET(50)] AS median_longitude
  FROM stationary_points GROUP BY device_id, ev_id
  HAVING (MAX(timestamp) - MIN(timestamp)) >= ${MIN_STOP_DURATION}
),
-- Filter points to remove outliers based on distance to median
filtered_points AS (
  SELECT sp.device_id, sp.timestamp, sp.latitude, sp.longitude, sp.ev_id
  FROM stationary_points sp
  JOIN initial_stops ist ON sp.device_id = ist.device_id AND sp.ev_id = ist.stop_id
  WHERE ST_DISTANCE(ST_GEOGPOINT(sp.longitude, sp.latitude), ST_GEOGPOINT(ist.median_longitude, ist.median_latitude)) <= ${DISTANCE_THRESHOLD}
),
stops AS (
  SELECT device_id, ev_id AS stop_id, MIN(timestamp) AS start_timestamp, MAX(timestamp) AS end_timestamp,
    APPROX_QUANTILES(latitude, 100)[OFFSET(50)] AS median_latitude, APPROX_QUANTILES(longitude, 100)[OFFSET(50)] AS median_longitude
  FROM filtered_points GROUP BY device_id, ev_id
  HAVING (MAX(timestamp) - MIN(timestamp)) >= ${MIN_STOP_DURATION}  -- check again after filtering
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY start_timestamp) AS stop_events
FROM (SELECT DISTINCT * FROM stops) ORDER BY device_id, stop_events;
EOF
)

echo ""
echo "Processing stops..."
bq query --use_legacy_sql=false "$QUERY"
echo "Done: ${LABEL} stops"
