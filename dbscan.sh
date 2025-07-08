#!/usr/bin/env bash
set -e

# Default configuration
PROJECT="job-accessibility"
INPUT_BUCKET="lbs-laltam"
OUTPUT_BUCKET="lbs-laltam"
DATASET="lbs_latam"
DBSCAN_DISTANCE=20        # meters
DBSCAN_MIN_POINTS=2       # minimum points for a cluster

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

echo "Running DBSCAN clustering with configuration:"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Country: $COUNTRY"
echo "DBSCAN distance threshold: $DBSCAN_DISTANCE meters"
echo "DBSCAN minimum points: $DBSCAN_MIN_POINTS"

# Build table names and paths
TNAME="${COUNTRY}_stops"
TABLE_NAME="${DATASET}.${TNAME}"
SOURCE_URI="gs://${INPUT_BUCKET}/stops/${COUNTRY}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${TNAME}"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/clusters/${COUNTRY}/part-*.parquet"

echo "Loading data from $SOURCE_URI to $TABLE_NAME..."
bq load \
  --source_format=PARQUET \
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

WITH
-- Initial clustering of points
clustered AS (
  SELECT
    device_id,
    stop_events,
    (start_timestamp / 1000) AS start_timestamp,
    (end_timestamp / 1000) AS end_timestamp,
    median_latitude,
    median_longitude,
    (end_timestamp / 1000 - start_timestamp / 1000) AS stop_duration,
    -- Use IFNULL to explicitly handle NULL values from ST_CLUSTERDBSCAN
    IFNULL(
      ST_CLUSTERDBSCAN(ST_GEOGPOINT(median_longitude, median_latitude), ${DBSCAN_DISTANCE}, ${DBSCAN_MIN_POINTS})
      OVER (PARTITION BY device_id),
      -1
    ) AS cluster_label
  FROM \`${TABLE}\`
),
-- Use window functions to calculate aggregates in a single pass
with_stats AS (
  SELECT
    device_id AS uid,
    stop_events AS stop_event,
    start_timestamp,
    end_timestamp,
    stop_duration,
    median_latitude AS stop_latitude,
    median_longitude AS stop_longitude,
    cluster_label,
    -- Calculate cluster counts using window function
    -- Assign cluster_counts=1 for cluster_label=-1
    CASE
      WHEN cluster_label = -1 THEN 1
      ELSE COUNT(*) OVER (PARTITION BY device_id, cluster_label)
    END AS cluster_counts,
    -- Calculate median coordinates within each cluster
    -- For cluster_label=-1, use the original coordinates
    CASE
      WHEN cluster_label = -1 THEN median_latitude
      ELSE PERCENTILE_CONT(median_latitude, 0.5) OVER (PARTITION BY device_id, cluster_label)
    END AS cluster_latitude,
    CASE
      WHEN cluster_label = -1 THEN median_longitude
      ELSE PERCENTILE_CONT(median_longitude, 0.5) OVER (PARTITION BY device_id, cluster_label)
    END AS cluster_longitude
  FROM clustered
)
SELECT *
FROM with_stats
ORDER BY uid, cluster_label, start_timestamp;
EOF
)

echo "Processing DBSCAN clustering..."
bq query --use_legacy_sql=false "$QUERY"

echo "Completed DBSCAN clustering for ${COUNTRY}"
