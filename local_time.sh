#!/usr/bin/env bash
set -e

FIXED_TZ_MODE=false

show_help() {
  echo "Usage: $0 [options] LABEL DEFAULT_TIMEZONE"
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket"
  echo "  -o, --output-bucket BUCKET Output GCS bucket"
  echo "  -d, --dataset DATASET      BigQuery dataset name"
  echo "  -t, --tz-table TABLE       Timezone lookup table"
  echo "  --fixed-tz                 Use fixed timezone (skip spatial join)"
  echo "  -h, --help                 Show this help message"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="$2"; shift 2 ;;
    -i|--input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    -o|--output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    -d|--dataset) DATASET="$2"; shift 2 ;;
    -t|--tz-table) TZ_TABLE="$2"; shift 2 ;;
    --fixed-tz) FIXED_TZ_MODE=true; shift ;;
    -h|--help) show_help ;;
    *) break ;;
  esac
done

[ "$#" -ne 2 ] && show_help
LABEL=$1
DEFAULT_TZ=$2

: "${PROJECT:?Must set PROJECT}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
TZ_TABLE="${TZ_TABLE:-${PROJECT}.${DATASET}.tz_codes}"

echo "Running Local Time Conversion with configuration:"
echo "  Project: $PROJECT"
echo "  Dataset: $DATASET"
echo "  Label: $LABEL"
echo "  Default timezone: $DEFAULT_TZ"
if $FIXED_TZ_MODE; then
  echo "  Mode: Fixed Timezone (skipping spatial join)"
else
  echo "  Mode: Spatial Join"
  echo "  Timezone table: $TZ_TABLE"
fi
echo ""

TABLE_NAME="${DATASET}.${LABEL}_clustered"
SOURCE_URI="gs://${OUTPUT_BUCKET}/stops/clusters/${LABEL}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${LABEL}_clustered"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/local/${LABEL}/part-*.parquet"

echo "Loading data from $SOURCE_URI..."
bq load --source_format=PARQUET ${TABLE_NAME} ${SOURCE_URI}

if $FIXED_TZ_MODE; then
  echo "Using fixed timezone: ${DEFAULT_TZ}"
  QUERY=$(cat <<EOF
EXPORT DATA OPTIONS (uri='${EXPORT_URI}', format='PARQUET', compression='SNAPPY', overwrite=true) AS
WITH stops_clustered AS (
  SELECT uid, stop_event,
    CAST(start_timestamp AS INT64) AS start_timestamp,
    CAST(end_timestamp AS INT64) AS end_timestamp,
    CAST(stop_duration AS INT64) AS stop_duration,
    stop_latitude, stop_longitude, cluster_label, cluster_counts, cluster_latitude, cluster_longitude
  FROM \`${TABLE}\`
),
stops_with_tz AS (
  SELECT *, '${DEFAULT_TZ}' AS timezone FROM stops_clustered
)
SELECT *,
  timezone AS inputed_timezone,
  DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}') AS stop_datetime,
  DATETIME(TIMESTAMP_SECONDS(end_timestamp), '${DEFAULT_TZ}') AS end_stop_datetime,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS date,
  EXTRACT(YEAR FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS year,
  EXTRACT(MONTH FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS month,
  EXTRACT(DAY FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS day,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS hour,
  EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) AS weekday,
  IF(EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP_SECONDS(start_timestamp), '${DEFAULT_TZ}')) IN (1, 7), TRUE, FALSE) AS weekend,
  EXTRACT(DATE FROM DATETIME(TIMESTAMP_SECONDS(end_timestamp), '${DEFAULT_TZ}')) AS end_date
FROM stops_with_tz;
EOF
)
else
  echo "Using spatial join with table: $TZ_TABLE"
  QUERY=$(cat <<EOF
EXPORT DATA OPTIONS (uri='${EXPORT_URI}', format='PARQUET', compression='SNAPPY', overwrite=true) AS
WITH stops_clustered AS (
  SELECT uid, stop_event,
    CAST(start_timestamp AS INT64) AS start_timestamp,
    CAST(end_timestamp AS INT64) AS end_timestamp,
    CAST(stop_duration AS INT64) AS stop_duration,
    stop_latitude, stop_longitude, cluster_label, cluster_counts, cluster_latitude, cluster_longitude
  FROM \`${TABLE}\`
),
stops_with_tz AS (
  SELECT s.*, t.tz AS timezone
  FROM stops_clustered s
  LEFT JOIN \`${TZ_TABLE}\` t ON ST_WITHIN(ST_GEOGPOINT(s.stop_longitude, s.stop_latitude), SAFE.ST_GEOGFROMTEXT(t.geometry))
)
SELECT *,
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
fi

echo "Running timezone conversion query..."
bq query --use_legacy_sql=false "$QUERY"

LOCAL_TABLE_NAME="${DATASET}.${LABEL}_local"
LOCAL_URI="gs://${OUTPUT_BUCKET}/stops/local/${LABEL}/*.parquet"

echo "Loading local times data to $LOCAL_TABLE_NAME..."
bq load --source_format=PARQUET ${LOCAL_TABLE_NAME} ${LOCAL_URI}

echo "Done: ${LABEL} local time"
