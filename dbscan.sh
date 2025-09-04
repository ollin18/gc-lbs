#!/usr/bin/env bash
set -e

# Display help
show_help() {
  echo "Usage: $0 [options] COUNTRY"
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID (default: $PROJECT)"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket (default: $INPUT_BUCKET)"
  echo "  -o, --output-bucket BUCKET Output GCS bucket (default: $OUTPUT_BUCKET)"
  echo "  -d, --dataset DATASET      BigQuery dataset name (default: $DATASET)"
  echo "  -dd, --dbscan-dist METERS  DBSCAN distance threshold in meters (default: $DBSCAN_DISTANCE)"
  echo "  -mp, --min-points COUNT    DBSCAN minimum points for a cluster (default: $DBSCAN_MIN_POINTS)"
  echo "  -h, --help                 Show this help message"
  exit 1
}

# Default configuration
# PROJECT="job-accessibility"
# INPUT_BUCKET="lbs-laltam"
# OUTPUT_BUCKET="lbs-laltam"
# DATASET="lbs_latam"
# DBSCAN_DISTANCE=20        # meters
# DBSCAN_MIN_POINTS=2       # minimum points for a cluster

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
    -dd|--dbscan-dist)
      DBSCAN_DISTANCE="$2"
      shift 2
      ;;
    -mp|--min-points)
      DBSCAN_MIN_POINTS="$2"
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

# Check that required environment variables are set, or fail with message
: "${PROJECT:?Must set PROJECT}"
: "${INPUT_BUCKET:?Must set INPUT_BUCKET}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
: "${DBSCAN_DISTANCE:?Must set DBSCAN_DISTANCE}"
: "${DBSCAN_MIN_POINTS:?Must set DBSCAN_MIN_POINTS}"

echo "Running DBSCAN clustering with configuration:"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Country: $COUNTRY"
echo "DBSCAN distance threshold: $DBSCAN_DISTANCE meters"
echo "DBSCAN minimum points: $DBSCAN_MIN_POINTS"

# Set vars
TNAME="${COUNTRY}_stops"
TABLE_NAME="${DATASET}.${TNAME}"
SOURCE_URI="gs://${INPUT_BUCKET}/stops/${COUNTRY}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/clusters/${COUNTRY}/part-*.parquet"


TNAME="${COUNTRY}_HW"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/dbscanfix/${COUNTRY}/part-*.parquet"

echo "Loading data from $SOURCE_URI to $TABLE_NAME..."
# bq load \
#   --source_format=PARQUET \
#   ${TABLE_NAME} \
#   ${SOURCE_URI}

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

WITH clustered AS (
  SELECT * EXCEPT(cluster_label, cluster_counts, cluster_latitude, cluster_longitude),
    -- Cluster stops using DBSCAN on the median coordinates (labels are scoped per uid).
    ST_CLUSTERDBSCAN(
      ST_GEOGPOINT(median_longitude, median_latitude),
      ${DBSCAN_DISTANCE},      -- eps (meters)
      ${DBSCAN_MIN_POINTS}     -- min_points
    ) OVER (PARTITION BY uid) AS cluster_label
  FROM `${TABLE}`
),

valid_stops AS (
  -- Filter out stops shorter than the threshold (seconds).
  SELECT *
  FROM clustered
  WHERE stop_duration >= ${MIN_STOP_DURATION}
),

cluster_stats AS (
  -- One row per (uid, cluster_label) with counts and an aggregate cluster center
  SELECT
    uid,
    cluster_label,                           -- NOTE: includes NULL for noise; that won't match in the join
    COUNT(*) AS cluster_counts,
    APPROX_QUANTILES(median_latitude, 100)[OFFSET(50)]  AS cluster_latitude,
    APPROX_QUANTILES(median_longitude, 100)[OFFSET(50)] AS cluster_longitude
  FROM valid_stops
  GROUP BY uid, cluster_label
)

SELECT
  v.uid,
  v.stop_event,
  v.start_timestamp,
  v.end_timestamp,
  v.stop_duration,
  v.stop_latitude,
  v.stop_longitude,
  v.timezone,
  v.inputed_timezone,
  v.stop_datetime,
  v.end_stop_datetime,
  v.date,
  v.year,
  v.month,
  v.day,
  v.hour,
  v.weekday,
  v.weekend,
  v,end_date,

  -- If no cluster_stats match (noise: cluster_label IS NULL), fallback to -1 and singletons
  IFNULL(cs.cluster_label, -1) AS cluster_label,
  IFNULL(cs.cluster_counts, 1) AS cluster_counts,
  IFNULL(cs.cluster_latitude,  v.median_latitude)  AS cluster_latitude,
  IFNULL(cs.cluster_longitude, v.median_longitude) AS cluster_longitude

FROM valid_stops v
LEFT JOIN cluster_stats cs
  ON v.uid = cs.uid
 AND v.cluster_label = cs.cluster_label

ORDER BY uid, start_timestamp;
EOF
)

echo "Processing DBSCAN clustering..."
bq query --use_legacy_sql=false "$QUERY"

# Create the final HW table

# COUNTRY="AR"
# DATASET="lbs_latam"
# OUTPUT_BUCKET="lbs-laltam"
# HW_TNAME="${COUNTRY}_dbscanfix"
# HW_TABLE_NAME="${DATASET}.${HW_TNAME}"
# HW_URI="gs://${OUTPUT_BUCKET}/stops/dbscanfix/${COUNTRY}/*.parquet"
# echo ${HW_URI}

echo "Loading home/work data from $HW_URI to $HW_TABLE_NAME..."
bq load \
  --source_format=PARQUET \
  ${HW_TABLE_NAME} \
  ${HW_URI}
echo "Completed DBSCAN clustering for ${COUNTRY}"

# Do the same for MX and BR in a for loop
# for COUNTRY in "BR" "MX"
# do
# DATASET="lbs_latam"
# OUTPUT_BUCKET="lbs-laltam"
# HW_TNAME="${COUNTRY}_dbscanfix"
# HW_TABLE_NAME="${DATASET}.${HW_TNAME}"
# HW_URI="gs://${OUTPUT_BUCKET}/stops/dbscanfix/${COUNTRY}/*.parquet"
# echo ${HW_URI}
#
# echo "Loading home/work data from $HW_URI to $HW_TABLE_NAME..."
# bq load \
#   --source_format=PARQUET \
#   ${HW_TABLE_NAME} \
#   ${HW_URI}
# echo "Completed DBSCAN clustering for ${COUNTRY}"
# done
#
