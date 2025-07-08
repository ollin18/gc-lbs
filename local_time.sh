#!/usr/bin/env bash
set -e

# Default configuration
PROJECT="job-accessibility"
INPUT_BUCKET="lbs-laltam"
OUTPUT_BUCKET="lbs-laltam"
DATASET="lbs_latam"
TZ_TABLE="${PROJECT}.${DATASET}.tz_codes"

# Display help
show_help() {
  echo "Usage: $0 [options] COUNTRY DEFAULT_TIMEZONE"
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID (default: $PROJECT)"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket (default: $INPUT_BUCKET)"
  echo "  -o, --output-bucket BUCKET Output GCS bucket (default: $OUTPUT_BUCKET)"
  echo "  -d, --dataset DATASET      BigQuery dataset name (default: $DATASET)"
  echo "  -t, --tz-table TABLE       Timezone lookup table (default: $TZ_TABLE)"
  echo "  -h, --help                 Show this help message"
  echo "Examples:"
  echo "  $0 CO America/Bogota       # Process Colombia with Bogota timezone as default"
  echo "  $0 MX America/Mexico_City  # Process Mexico with Mexico City timezone as default"
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
    -t|--tz-table)
      TZ_TABLE="$2"
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
  echo "Error: COUNTRY and DEFAULT_TIMEZONE arguments are required"
  show_help
fi

COUNTRY=$1
DEFAULT_TZ=$2

echo "Running Local Time Conversion with configuration:"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Country: $COUNTRY"
echo "Default Timezone: $DEFAULT_TZ"
echo "Timezone Table: $TZ_TABLE"

# Build table names and paths
TNAME="${COUNTRY}_clustered_stops"
TABLE_NAME="${DATASET}.${TNAME}"
SOURCE_URI="gs://${INPUT_BUCKET}/stops/clusters/${COUNTRY}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/local/${COUNTRY}/part-*.parquet"

echo "Loading data from $SOURCE_URI to $TABLE_NAME..."
bq load \
  --source_format=PARQUET \
  ${TABLE_NAME} \
  ${SOURCE_URI}

QUERY=$(cat <<EOF
EXPORT DATA
OPTIONS (
  uri='${EXPORT_URI}',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
)
AS

WITH stops_clustered AS (
  SELECT
    uid,
    stop_event,
    CAST(start_timestamp AS INT64) AS start_timestamp,
    CAST(end_timestamp AS INT64) AS end_timestamp,
    CAST(stop_duration AS INT64) AS stop_duration,
    stop_latitude,
    stop_longitude,
    cluster_label,
    cluster_counts,
    cluster_latitude,
    cluster_longitude
  FROM \`${TABLE}\`
),
stops_with_tz AS (
  SELECT
    s.*,
    t.tz AS timezone
  FROM stops_clustered s
  LEFT JOIN (
    SELECT *
    FROM \`${TZ_TABLE}\`
    WHERE country_code = '${COUNTRY}'
  ) t
    ON ST_WITHIN(
         ST_GEOGPOINT(s.stop_longitude, s.stop_latitude),
         SAFE.ST_GEOGFROMTEXT(t.geometry)
       )
)

SELECT
  *,
  -- Convert start_timestamp (ms) to local datetime using the joined timezone.
  IFNULL(timezone, '${DEFAULT_TZ}') AS inputed_timezone,
  DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}')) AS stop_datetime,
  DATETIME(TIMESTAMP_SECONDS(end_timestamp), IFNULL(timezone, '${DEFAULT_TZ}')) AS end_stop_datetime,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS date,
  EXTRACT(YEAR FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS year,
  EXTRACT(MONTH FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS month,
  EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS day,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS hour,
  EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS weekday,
  IF(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) IN (1, 7), TRUE, FALSE) AS weekend,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_SECONDS(end_timestamp), IFNULL(timezone, '${DEFAULT_TZ}'))) AS end_date
FROM stops_with_tz;
EOF
)

echo "Running timezone conversion query..."
bq query --use_legacy_sql=false "$QUERY"

# Create the local times table
LOCAL_TNAME="${COUNTRY}_local"
LOCAL_TABLE_NAME="${DATASET}.${LOCAL_TNAME}"
LOCAL_URI="gs://${OUTPUT_BUCKET}/stops/local/${COUNTRY}/*.parquet"

echo "Loading local times data from $LOCAL_URI to $LOCAL_TABLE_NAME..."
bq load \
  --source_format=PARQUET \
  ${LOCAL_TABLE_NAME} \
  ${LOCAL_URI}

echo "Completed local time conversion for ${COUNTRY}"
