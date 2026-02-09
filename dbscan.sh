#!/usr/bin/env bash
set -e

# Display help
show_help() {
  echo "Usage: $0 [options] LABEL"
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket"
  echo "  -o, --output-bucket BUCKET Output GCS bucket"
  echo "  -d, --dataset DATASET      BigQuery dataset name"
  echo "  -dd, --dbscan-dist METERS  DBSCAN distance threshold in meters (default: 20)"
  echo "  -mp, --min-points COUNT    DBSCAN minimum points for a cluster (default: 2)"
  echo "  -h, --help                 Show this help message"
  exit 1
}

# Parse command line options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="$2"; shift 2 ;;
    -i|--input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    -o|--output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    -d|--dataset) DATASET="$2"; shift 2 ;;
    -dd|--dbscan-dist) DBSCAN_DISTANCE="$2"; shift 2 ;;
    -mp|--min-points) DBSCAN_MIN_POINTS="$2"; shift 2 ;;
    -h|--help) show_help ;;
    *) break ;;
  esac
done

[ "$#" -ne 1 ] && show_help
LABEL=$1

: "${PROJECT:?Must set PROJECT}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
: "${DBSCAN_DISTANCE:=20}"
: "${DBSCAN_MIN_POINTS:=2}"

echo "Running DBSCAN clustering with configuration:"
echo "  Project: $PROJECT"
echo "  Dataset: $DATASET"
echo "  Label: $LABEL"
echo "  Distance threshold: ${DBSCAN_DISTANCE} meters"
echo "  Minimum points: $DBSCAN_MIN_POINTS"
echo ""

TABLE_NAME="${DATASET}.${LABEL}_stops"
SOURCE_URI="gs://${OUTPUT_BUCKET}/stops/${LABEL}/*.parquet"
TABLE="${PROJECT}.${DATASET}.${LABEL}_stops"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/clusters/${LABEL}/part-*.parquet"

echo "Loading data from $SOURCE_URI..."
bq load --source_format=PARQUET ${TABLE_NAME} ${SOURCE_URI}

QUERY=$(cat <<EOF
EXPORT DATA OPTIONS (uri='${EXPORT_URI}', format='PARQUET', compression='SNAPPY', overwrite=true) AS
WITH clustered AS (
  SELECT
    device_id,
    stop_events,
    start_timestamp,
    end_timestamp,
    median_latitude,
    median_longitude,
    (end_timestamp - start_timestamp) AS stop_duration,
    IFNULL(
      ST_CLUSTERDBSCAN(ST_GEOGPOINT(median_longitude, median_latitude), ${DBSCAN_DISTANCE}, ${DBSCAN_MIN_POINTS})
      OVER (PARTITION BY device_id),
      -1
    ) AS raw_cluster_label
  FROM \`${TABLE}\`
),
cluster_centroids AS (
  SELECT device_id, raw_cluster_label,
    AVG(median_latitude) AS centroid_lat,
    AVG(median_longitude) AS centroid_lon,
    COUNT(*) AS cluster_size
  FROM clustered WHERE raw_cluster_label >= 0
  GROUP BY device_id, raw_cluster_label
),
deterministic_labels AS (
  SELECT device_id, raw_cluster_label, centroid_lat, centroid_lon, cluster_size,
    DENSE_RANK() OVER (PARTITION BY device_id ORDER BY centroid_lat, centroid_lon) - 1 AS cluster_label
  FROM cluster_centroids
),
with_deterministic_labels AS (
  SELECT c.*,
    CASE WHEN c.raw_cluster_label = -1 THEN -1 ELSE COALESCE(d.cluster_label, -1) END AS cluster_label
  FROM clustered c
  LEFT JOIN deterministic_labels d ON c.device_id = d.device_id AND c.raw_cluster_label = d.raw_cluster_label
),
with_stats AS (
  SELECT
    device_id AS uid, stop_events AS stop_event, start_timestamp, end_timestamp, stop_duration,
    median_latitude AS stop_latitude, median_longitude AS stop_longitude, cluster_label,
    CASE WHEN cluster_label = -1 THEN 1 ELSE COUNT(*) OVER (PARTITION BY device_id, cluster_label) END AS cluster_counts,
    CASE WHEN cluster_label = -1 THEN median_latitude ELSE PERCENTILE_CONT(median_latitude, 0.5) OVER (PARTITION BY device_id, cluster_label) END AS cluster_latitude,
    CASE WHEN cluster_label = -1 THEN median_longitude ELSE PERCENTILE_CONT(median_longitude, 0.5) OVER (PARTITION BY device_id, cluster_label) END AS cluster_longitude
  FROM with_deterministic_labels
),
final_deduplicated AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY uid, stop_event ORDER BY start_timestamp) as row_num
    FROM with_stats
  ) WHERE row_num = 1
)
SELECT * EXCEPT(row_num) FROM final_deduplicated ORDER BY uid, cluster_label, start_timestamp;
EOF
)

echo "Processing DBSCAN clustering..."
bq query --use_legacy_sql=false "$QUERY"
echo "Done: ${LABEL} clustering"
