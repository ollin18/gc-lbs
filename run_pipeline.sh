#!/usr/bin/env bash
set -e

# Step flags (all enabled by default)
RUN_STOPS=true
RUN_DBSCAN=true
RUN_TIMEZONE=true
RUN_HW=true

# Input options
INPUT_FORMAT="auto"
CSV_SEPARATOR=$'\t'
CSV_HEADER=true
FIXED_TZ=false
INPUT_PATH=""

# Column mapping
declare -A COLUMN_MAP=(
  ["device_id"]="device_id"
  ["latitude"]="latitude"
  ["longitude"]="longitude"
  ["timestamp"]="timestamp"
)

show_help() {
  echo "Usage: $0 [options] LABEL TIMEZONE"
  echo
  echo "Arguments:"
  echo "  LABEL      Label for output tables (e.g., cdmx, CO, MX)"
  echo "  TIMEZONE   Timezone (e.g., America/Mexico_City)"
  echo
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket"
  echo "  -o, --output-bucket BUCKET Output GCS bucket"
  echo "  -d, --dataset DATASET      BigQuery dataset name"
  echo "  --input-path PATH          Input path (overrides bucket/label pattern)"
  echo "  --format FORMAT            Input format: auto, parquet, csv (default: auto)"
  echo "  --csv-sep SEP              CSV separator (default: tab)"
  echo "  --no-header                CSV has no header row"
  echo "  --col MAPPING              Column mapping (e.g., --col device_id=aid)"
  echo "  --fixed-tz                 Use fixed timezone (skip spatial join)"
  echo "  --skip-stops               Skip stops detection"
  echo "  --skip-dbscan              Skip DBSCAN clustering"
  echo "  --skip-timezone            Skip timezone conversion"
  echo "  --skip-hw                  Skip home/work classification"
  echo "  -h, --help                 Show this help"
  exit 1
}

# Parse options
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project) PROJECT="$2"; shift 2 ;;
    -i|--input-bucket) INPUT_BUCKET="$2"; shift 2 ;;
    -o|--output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
    -d|--dataset) DATASET="$2"; shift 2 ;;
    --input-path) INPUT_PATH="$2"; shift 2 ;;
    --format) INPUT_FORMAT="$2"; shift 2 ;;
    --csv-sep) CSV_SEPARATOR="$2"; shift 2 ;;
    --no-header) CSV_HEADER=false; shift ;;
    --col) IFS='=' read -r std actual <<< "$2"; COLUMN_MAP["$std"]="$actual"; shift 2 ;;
    --fixed-tz) FIXED_TZ=true; shift ;;
    --skip-stops) RUN_STOPS=false; shift ;;
    --skip-dbscan) RUN_DBSCAN=false; shift ;;
    --skip-timezone) RUN_TIMEZONE=false; shift ;;
    --skip-hw) RUN_HW=false; shift ;;
    -h|--help) show_help ;;
    -*) echo "Unknown option: $1"; show_help ;;
    *) POSITIONAL_ARGS+=("$1"); shift ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}"
if [ $# -ne 2 ]; then
  echo "Error: Must provide LABEL and TIMEZONE"
  show_help
fi

LABEL="$1"
TIMEZONE="$2"

# Load config file if exists
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CONFIG_FILE="${SCRIPT_DIR}/pipeline_config.json"

if [ -f "$CONFIG_FILE" ]; then
  echo "Loading config from $CONFIG_FILE..."
  : "${PROJECT:=$(jq -r '.project' "$CONFIG_FILE")}"
  : "${INPUT_BUCKET:=$(jq -r '.input_bucket' "$CONFIG_FILE")}"
  : "${OUTPUT_BUCKET:=$(jq -r '.output_bucket' "$CONFIG_FILE")}"
  : "${DATASET:=$(jq -r '.dataset' "$CONFIG_FILE")}"

  # Try to load label-specific params
  if jq -e ".countries.${LABEL}" "$CONFIG_FILE" > /dev/null 2>&1; then
    DISTANCE_THRESHOLD=$(jq -r ".countries.${LABEL}.stops.distance_threshold" "$CONFIG_FILE")
    TIME_THRESHOLD=$(jq -r ".countries.${LABEL}.stops.time_threshold" "$CONFIG_FILE")
    MIN_STOP_DURATION=$(jq -r ".countries.${LABEL}.stops.min_stop_duration" "$CONFIG_FILE")
    DBSCAN_DISTANCE=$(jq -r ".countries.${LABEL}.dbscan.distance" "$CONFIG_FILE")
    DBSCAN_MIN_POINTS=$(jq -r ".countries.${LABEL}.dbscan.min_points" "$CONFIG_FILE")
    HOME_MIN_DAYS=$(jq -r ".countries.${LABEL}.home_work.home_min_days" "$CONFIG_FILE")
    HOME_HOUR_EVENING=$(jq -r ".countries.${LABEL}.home_work.home_hour_evening" "$CONFIG_FILE")
    HOME_HOUR_MORNING=$(jq -r ".countries.${LABEL}.home_work.home_hour_morning" "$CONFIG_FILE")
    WORK_MIN_DAYS=$(jq -r ".countries.${LABEL}.home_work.work_min_days" "$CONFIG_FILE")
    WORK_HOUR_START=$(jq -r ".countries.${LABEL}.home_work.work_hour_start" "$CONFIG_FILE")
    WORK_HOUR_END=$(jq -r ".countries.${LABEL}.home_work.work_hour_end" "$CONFIG_FILE")
    MIN_DISTANCE_HOME_WORK=$(jq -r ".countries.${LABEL}.home_work.min_distance_home_work" "$CONFIG_FILE")
  fi
fi

# Defaults
: "${DISTANCE_THRESHOLD:=20}"
: "${TIME_THRESHOLD:=3600}"
: "${MIN_STOP_DURATION:=300}"
: "${DBSCAN_DISTANCE:=20}"
: "${DBSCAN_MIN_POINTS:=2}"
: "${HOME_MIN_DAYS:=14}"
: "${HOME_HOUR_EVENING:=19}"
: "${HOME_HOUR_MORNING:=8}"
: "${WORK_MIN_DAYS:=8}"
: "${WORK_HOUR_START:=9}"
: "${WORK_HOUR_END:=18}"
: "${MIN_DISTANCE_HOME_WORK:=50}"

# Validate
: "${PROJECT:?Must set PROJECT}"
: "${OUTPUT_BUCKET:?Must set OUTPUT_BUCKET}"
: "${DATASET:?Must set DATASET}"
[ -z "$INPUT_PATH" ] && : "${INPUT_BUCKET:?Must set INPUT_BUCKET or --input-path}"

# Export
export PROJECT INPUT_BUCKET OUTPUT_BUCKET DATASET INPUT_PATH
export DISTANCE_THRESHOLD TIME_THRESHOLD MIN_STOP_DURATION
export DBSCAN_DISTANCE DBSCAN_MIN_POINTS
export HOME_MIN_DAYS HOME_HOUR_EVENING HOME_HOUR_MORNING
export WORK_MIN_DAYS WORK_HOUR_START WORK_HOUR_END MIN_DISTANCE_HOME_WORK
export INPUT_FORMAT CSV_SEPARATOR CSV_HEADER FIXED_TZ
export COL_DEVICE_ID="${COLUMN_MAP[device_id]}"
export COL_LATITUDE="${COLUMN_MAP[latitude]}"
export COL_LONGITUDE="${COLUMN_MAP[longitude]}"
export COL_TIMESTAMP="${COLUMN_MAP[timestamp]}"

echo "=========================================================="
echo "LBS Pipeline - Label: $LABEL"
echo "=========================================================="
echo "Project: $PROJECT | Dataset: $DATASET"
echo "Timezone: $TIMEZONE | Fixed TZ: $FIXED_TZ"
[ -n "$INPUT_PATH" ] && echo "Input: $INPUT_PATH" || echo "Input: gs://$INPUT_BUCKET/$LABEL/"
echo "=========================================================="

COMMON="--project $PROJECT --input-bucket $INPUT_BUCKET --output-bucket $OUTPUT_BUCKET --dataset $DATASET"

if $RUN_STOPS; then
  echo "Step 1: Stops detection..."
  ${SCRIPT_DIR}/process_yearly_stays.sh $COMMON $LABEL
fi

if $RUN_DBSCAN; then
  echo "Step 2: DBSCAN clustering..."
  ${SCRIPT_DIR}/dbscan.sh $COMMON $LABEL
fi

if $RUN_TIMEZONE; then
  echo "Step 3: Timezone conversion..."
  FIXED_FLAG=""; $FIXED_TZ && FIXED_FLAG="--fixed-tz"
  ${SCRIPT_DIR}/local_time.sh $COMMON $FIXED_FLAG $LABEL $TIMEZONE
fi

if $RUN_HW; then
  echo "Step 4: Home/Work classification..."
  ${SCRIPT_DIR}/get_hw.sh $COMMON $LABEL
fi

echo "=========================================================="
echo "Done! Tables: ${DATASET}.${LABEL}_*"
echo "=========================================================="
