#!/usr/bin/env bash
set -e

show_help() {
  echo "Usage: $0 [options] LABEL"
  echo "Options:"
  echo "  -p, --project PROJECT           Google Cloud project ID"
  echo "  -i, --input-bucket BUCKET       Input GCS bucket"
  echo "  -o, --output-bucket BUCKET      Output GCS bucket"
  echo "  -d, --dataset DATASET           BigQuery dataset name"
  echo "  -hd, --home-days DAYS           Minimum unique days for home (default: 14)"
  echo "  -he, --home-evening HOUR        Hour when evening starts (default: 19)"
  echo "  -hm, --home-morning HOUR        Hour when morning ends (default: 8)"
  echo "  -wd, --work-days DAYS           Minimum unique days for work (default: 8)"
  echo "  -ws, --work-start HOUR          Work day start hour (default: 9)"
  echo "  -we, --work-end HOUR            Work day end hour (default: 18)"
  echo "  -md, --min-distance METERS      Min distance between home and work (default: 50)"
  echo "  -h, --help                      Show this help message"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="$2"; shift 2 ;;
    -i|--input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    -o|--output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    -d|--dataset) DATASET="$2"; shift 2 ;;
    -hd|--home-days) HOME_MIN_DAYS="$2"; shift 2 ;;
    -he|--home-evening) HOME_HOUR_EVENING="$2"; shift 2 ;;
    -hm|--home-morning) HOME_HOUR_MORNING="$2"; shift 2 ;;
    -wd|--work-days) WORK_MIN_DAYS="$2"; shift 2 ;;
    -ws|--work-start) WORK_HOUR_START="$2"; shift 2 ;;
    -we|--work-end) WORK_HOUR_END="$2"; shift 2 ;;
    -md|--min-distance) MIN_DISTANCE_HOME_WORK="$2"; shift 2 ;;
    -h|--help) show_help ;;
    *) break ;;
  esac
done

[ "$#" -ne 1 ] && show_help
LABEL=$1

: "${PROJECT:?Must set PROJECT}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
: "${HOME_MIN_DAYS:=14}"
: "${HOME_HOUR_EVENING:=19}"
: "${HOME_HOUR_MORNING:=8}"
: "${WORK_MIN_DAYS:=8}"
: "${WORK_HOUR_START:=9}"
: "${WORK_HOUR_END:=18}"
: "${MIN_DISTANCE_HOME_WORK:=50}"

echo "Running Home/Work Classification with configuration:"
echo "  Project: $PROJECT"
echo "  Dataset: $DATASET"
echo "  Label: $LABEL"
echo "  Home parameters:"
echo "    - Minimum unique days: $HOME_MIN_DAYS"
echo "    - Evening hour: $HOME_HOUR_EVENING"
echo "    - Morning hour: $HOME_HOUR_MORNING"
echo "  Work parameters:"
echo "    - Minimum unique days: $WORK_MIN_DAYS"
echo "    - Work hours: ${WORK_HOUR_START}-${WORK_HOUR_END}"
echo "    - Min distance from home: ${MIN_DISTANCE_HOME_WORK} meters"
echo ""

TABLE="${PROJECT}.${DATASET}.${LABEL}_local"
EXPORT_URI="gs://${OUTPUT_BUCKET}/stops/HW/${LABEL}/part-*.parquet"

QUERY=$(cat <<EOF
EXPORT DATA OPTIONS (uri='${EXPORT_URI}', format='PARQUET', compression='SNAPPY', overwrite=true) AS
WITH
base AS (
  SELECT * FROM \`${TABLE}\`
),
-- Count unique days each cluster is visited (global, not per-year)
cluster_day_counts AS (
  SELECT uid, cluster_label, COUNT(DISTINCT date) AS unique_days_visited
  FROM base WHERE cluster_label >= 0 GROUP BY uid, cluster_label
),
-- Home candidates: weekends + weekdays during home hours (use AVG for coordinates)
home_candidates AS (
  SELECT uid, cluster_label,
    AVG(cluster_latitude) AS cluster_latitude,
    AVG(cluster_longitude) AS cluster_longitude,
    COUNT(*) AS home_time_visits,
    COUNT(DISTINCT date) AS home_time_unique_days
  FROM base
  WHERE cluster_label >= 0 AND
    ((weekend = TRUE) OR (weekend = FALSE AND (hour >= ${HOME_HOUR_EVENING} OR hour < ${HOME_HOUR_MORNING})))
  GROUP BY uid, cluster_label
),
-- Identify home with highest visits meeting minimum days threshold
home_locations AS (
  SELECT h.*, dc.unique_days_visited,
    ROW_NUMBER() OVER (PARTITION BY h.uid ORDER BY h.home_time_visits DESC, h.home_time_unique_days DESC, h.cluster_label) AS home_rank
  FROM home_candidates h
  JOIN cluster_day_counts dc ON h.uid = dc.uid AND h.cluster_label = dc.cluster_label
  WHERE dc.unique_days_visited >= ${HOME_MIN_DAYS}
),
final_home_locations AS (
  SELECT uid, cluster_label AS home_cluster_label, cluster_latitude AS home_latitude, cluster_longitude AS home_longitude,
    home_time_visits, unique_days_visited
  FROM home_locations WHERE home_rank = 1
),
-- Work candidates: weekdays during work hours (use AVG for coordinates)
work_candidates AS (
  SELECT uid, cluster_label,
    AVG(cluster_latitude) AS cluster_latitude,
    AVG(cluster_longitude) AS cluster_longitude,
    COUNT(*) AS work_time_visits,
    COUNT(DISTINCT date) AS work_time_unique_days
  FROM base
  WHERE cluster_label >= 0 AND weekend = FALSE AND hour >= ${WORK_HOUR_START} AND hour <= ${WORK_HOUR_END}
  GROUP BY uid, cluster_label
),
-- Filter work candidates: not home, sufficient distance, enough days
work_candidates_filtered AS (
  SELECT w.*, h.home_cluster_label,
    ST_DISTANCE(ST_GEOGPOINT(w.cluster_longitude, w.cluster_latitude), ST_GEOGPOINT(h.home_longitude, h.home_latitude)) AS distance_from_home,
    CASE
      WHEN h.home_cluster_label IS NULL THEN FALSE
      WHEN w.cluster_label = h.home_cluster_label THEN FALSE
      WHEN w.work_time_unique_days < ${WORK_MIN_DAYS} THEN FALSE
      ELSE TRUE
    END AS is_valid_work_candidate
  FROM work_candidates w
  LEFT JOIN final_home_locations h ON w.uid = h.uid
),
work_locations AS (
  SELECT uid, cluster_label AS work_cluster_label, cluster_latitude AS work_latitude, cluster_longitude AS work_longitude, work_time_visits,
    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY work_time_visits DESC, work_time_unique_days DESC, cluster_label) AS work_rank
  FROM work_candidates_filtered
  WHERE is_valid_work_candidate = TRUE AND distance_from_home >= ${MIN_DISTANCE_HOME_WORK}
),
final_work_locations AS (
  SELECT uid, work_cluster_label, work_latitude, work_longitude, work_time_visits
  FROM work_locations WHERE work_rank = 1
),
-- Assign location type labels (join on uid only, not on cluster_label)
location_type_assignment AS (
  SELECT b.*,
    CASE
      WHEN b.cluster_label = -1 THEN 'O'  -- Noise points are always 'Other'
      WHEN h.home_cluster_label IS NOT NULL AND b.cluster_label = h.home_cluster_label THEN 'H'
      WHEN w.work_cluster_label IS NOT NULL AND b.cluster_label = w.work_cluster_label THEN 'W'
      ELSE 'O'
    END AS location_type
  FROM base b
  LEFT JOIN final_home_locations h ON b.uid = h.uid
  LEFT JOIN final_work_locations w ON b.uid = w.uid
),
final_result AS (
  SELECT *,
    CASE
      WHEN location_type = 'H' THEN 1
      WHEN location_type = 'W' THEN 1
      ELSE -1
    END AS location_label
  FROM location_type_assignment
)
SELECT
  uid, stop_event, start_timestamp, end_timestamp, stop_duration,
  stop_latitude, stop_longitude, cluster_label, cluster_counts, cluster_latitude, cluster_longitude,
  timezone, inputed_timezone, stop_datetime, end_stop_datetime,
  date, year, month, day, hour, weekday, weekend, end_date, location_type
FROM location_type_assignment
ORDER BY uid, date, start_timestamp;
EOF
)

echo "Running home/work classification query..."
bq query --use_legacy_sql=false "$QUERY"

HW_TABLE_NAME="${DATASET}.${LABEL}_HW"
HW_URI="gs://${OUTPUT_BUCKET}/stops/HW/${LABEL}/*.parquet"

echo "Loading home/work data to $HW_TABLE_NAME..."
bq load --source_format=PARQUET ${HW_TABLE_NAME} ${HW_URI}

echo "Done: ${LABEL} home/work"
