#!/usr/bin/env bash
set -e

# Step flags (all enabled by default)
RUN_STOPS=true
RUN_DBSCAN=true
RUN_TIMEZONE=true
RUN_HW=true

# Input format and fixed timezone flags
INPUT_FORMAT="auto"  # auto, parquet, csv
CSV_SEPARATOR=$'\t'  # Default: tab
CSV_HEADER=true
FIXED_TZ=false

# Column mapping (standard_name=actual_name)
declare -A COLUMN_MAP=(
  ["device_id"]="device_id"
  ["latitude"]="latitude"
  ["longitude"]="longitude"
  ["timestamp"]="timestamp"
)

# Help function
show_help() {
  echo "Usage: $0 [options] COUNTRY YEAR TIMEZONE"
  echo
  echo "Runs the complete LBS data processing pipeline for a country and year."
  echo
  echo "Arguments:"
  echo "  COUNTRY    Country code (e.g., CO for Colombia)"
  echo "  YEAR       Year to process (e.g., 2022)"
  echo "  TIMEZONE   Default timezone (e.g., America/Bogota)"
  echo
  echo "Options:"
  echo "  -p, --project PROJECT      Google Cloud project ID (default: $PROJECT)"
  echo "  -i, --input-bucket BUCKET  Input GCS bucket (default: $INPUT_BUCKET)"
  echo "  -o, --output-bucket BUCKET Output GCS bucket (default: $OUTPUT_BUCKET)"
  echo "  -d, --dataset DATASET      BigQuery dataset name (default: $DATASET)"
  echo "  --format FORMAT            Input format: auto, parquet, csv (default: auto)"
  echo "  --csv-sep SEP              CSV separator (default: tab)"
  echo "  --no-header                CSV has no header row"
  echo "  --col MAPPING              Column mapping (e.g., --col device_id=aid)"
  echo "  --fixed-tz                 Use fixed timezone (skip spatial join)"
  echo "  --skip-stops               Skip the stops detection step"
  echo "  --skip-dbscan              Skip the DBSCAN clustering step"
  echo "  --skip-timezone            Skip the timezone conversion step"
  echo "  --skip-hw                  Skip the home/work classification step"
  echo "  -h, --help                 Show this help message"
  exit 1
}

# Parse command line options
POSITIONAL_ARGS=()

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
    --col)
      # Parse column mapping: device_id=aid
      IFS='=' read -r std_col actual_col <<< "$2"
      COLUMN_MAP["$std_col"]="$actual_col"
      shift 2
      ;;
    --fixed-tz)
      FIXED_TZ=true
      shift
      ;;
    --skip-stops)
      RUN_STOPS=false
      shift
      ;;
    --skip-dbscan)
      RUN_DBSCAN=false
      shift
      ;;
    --skip-timezone)
      RUN_TIMEZONE=false
      shift
      ;;
    --skip-hw)
      RUN_HW=false
      shift
      ;;
    -h|--help)
      show_help
      ;;
    -*)
      echo "Unknown option: $1"
      show_help
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

# Restore positional args
set -- "${POSITIONAL_ARGS[@]}"

# Validate positional arguments
if [ $# -ne 3 ]; then
  echo "Error: Must provide COUNTRY, YEAR, and TIMEZONE."
  show_help
fi

COUNTRY="$1"
YEAR="$2"
TIMEZONE="$3"


CONFIG_FILE="pipeline_config.json"

# Load global config
PROJECT=$(jq -r '.project' "$CONFIG_FILE")
INPUT_BUCKET=$(jq -r '.input_bucket' "$CONFIG_FILE")
OUTPUT_BUCKET=$(jq -r '.output_bucket' "$CONFIG_FILE")
DATASET=$(jq -r '.dataset' "$CONFIG_FILE")

# Load country-specific config
TIMEZONE=$(jq -r ".countries.${COUNTRY}.timezone" "$CONFIG_FILE")

# Stops parameters
DISTANCE_THRESHOLD=$(jq -r ".countries.${COUNTRY}.stops.distance_threshold" "$CONFIG_FILE")
TIME_THRESHOLD=$(jq -r ".countries.${COUNTRY}.stops.time_threshold" "$CONFIG_FILE")
MIN_STOP_DURATION=$(jq -r ".countries.${COUNTRY}.stops.min_stop_duration" "$CONFIG_FILE")

# DBSCAN parameters
DBSCAN_DISTANCE=$(jq -r ".countries.${COUNTRY}.dbscan.distance" "$CONFIG_FILE")
DBSCAN_MIN_POINTS=$(jq -r ".countries.${COUNTRY}.dbscan.min_points" "$CONFIG_FILE")

# Home/Work parameters
HOME_MIN_DAYS=$(jq -r ".countries.${COUNTRY}.home_work.home_min_days" "$CONFIG_FILE")
HOME_HOUR_EVENING=$(jq -r ".countries.${COUNTRY}.home_work.home_hour_evening" "$CONFIG_FILE")
HOME_HOUR_MORNING=$(jq -r ".countries.${COUNTRY}.home_work.home_hour_morning" "$CONFIG_FILE")
WORK_MIN_DAYS=$(jq -r ".countries.${COUNTRY}.home_work.work_min_days" "$CONFIG_FILE")
WORK_HOUR_START=$(jq -r ".countries.${COUNTRY}.home_work.work_hour_start" "$CONFIG_FILE")
WORK_HOUR_END=$(jq -r ".countries.${COUNTRY}.home_work.work_hour_end" "$CONFIG_FILE")
MIN_DISTANCE_HOME_WORK=$(jq -r ".countries.${COUNTRY}.home_work.min_distance_home_work" "$CONFIG_FILE")

export PROJECT INPUT_BUCKET OUTPUT_BUCKET DATASET
export DISTANCE_THRESHOLD TIME_THRESHOLD MIN_STOP_DURATION
export DBSCAN_DISTANCE DBSCAN_MIN_POINTS
export HOME_MIN_DAYS HOME_HOUR_EVENING HOME_HOUR_MORNING
export WORK_MIN_DAYS WORK_HOUR_START WORK_HOUR_END MIN_DISTANCE_HOME_WORK
export INPUT_FORMAT CSV_SEPARATOR CSV_HEADER FIXED_TZ
# Export column mappings
export COL_DEVICE_ID="${COLUMN_MAP[device_id]}"
export COL_LATITUDE="${COLUMN_MAP[latitude]}"
export COL_LONGITUDE="${COLUMN_MAP[longitude]}"
export COL_TIMESTAMP="${COLUMN_MAP[timestamp]}"

echo "=========================================================="
echo "LBS Data Processing Pipeline"
echo "=========================================================="
echo "Configuration:"
echo "  Project: $PROJECT"
echo "  Dataset: $DATASET"
echo "  Country: $COUNTRY"
echo "  Year: $YEAR"
echo "  Timezone: $TIMEZONE"
echo "  Input Bucket: $INPUT_BUCKET"
echo "  Output Bucket: $OUTPUT_BUCKET"
echo "  Input Format: $INPUT_FORMAT"
echo "  Fixed Timezone: $(if $FIXED_TZ; then echo "Yes"; else echo "No (spatial join)"; fi)"
if [ "$INPUT_FORMAT" = "csv" ]; then
  echo "  CSV Separator: '${CSV_SEPARATOR}'"
  echo "  CSV Header: $CSV_HEADER"
fi
echo "  Column Mapping:"
echo "    device_id -> $COL_DEVICE_ID"
echo "    latitude -> $COL_LATITUDE"
echo "    longitude -> $COL_LONGITUDE"
echo "    timestamp -> $COL_TIMESTAMP"
echo "=========================================================="
echo "Steps to run:"
echo "  Stops Detection: $(if $RUN_STOPS; then echo "Yes"; else echo "Skip"; fi)"
echo "  DBSCAN Clustering: $(if $RUN_DBSCAN; then echo "Yes"; else echo "Skip"; fi)"
echo "  Timezone Conversion: $(if $RUN_TIMEZONE; then echo "Yes"; else echo "Skip"; fi)"
echo "  Home/Work Classification: $(if $RUN_HW; then echo "Yes"; else echo "Skip"; fi)"
echo "=========================================================="

# Define script paths (use the directory this script is in)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
STOPS_SCRIPT="${SCRIPT_DIR}/process_yearly_stays.sh"
DBSCAN_SCRIPT="${SCRIPT_DIR}/dbscan.sh"
TIMEZONE_SCRIPT="${SCRIPT_DIR}/local_time.sh"
HW_SCRIPT="${SCRIPT_DIR}/get_hw.sh"

# Make scripts executable
chmod +x "${STOPS_SCRIPT}" "${DBSCAN_SCRIPT}" "${TIMEZONE_SCRIPT}" "${HW_SCRIPT}"

# Common parameters for all scripts
COMMON_PARAMS="--project ${PROJECT} --input-bucket ${INPUT_BUCKET} --output-bucket ${OUTPUT_BUCKET} --dataset ${DATASET}"

# Step 1: Process stops
if $RUN_STOPS; then
  echo "Step 1: Running stops detection..."
  ${STOPS_SCRIPT} ${COMMON_PARAMS} ${COUNTRY} ${YEAR}
  echo "Stops detection completed."
else
  echo "Step 1: Skipping stops detection."
fi

# Step 2: Run DBSCAN clustering
if $RUN_DBSCAN; then
  echo "Step 2: Running DBSCAN clustering..."
  ${DBSCAN_SCRIPT} ${COMMON_PARAMS} ${COUNTRY}
  echo "DBSCAN clustering completed."
else
  echo "Step 2: Skipping DBSCAN clustering."
fi

# Step 3: Convert to local timezone
if $RUN_TIMEZONE; then
  echo "Step 3: Running timezone conversion..."
  FIXED_TZ_FLAG=""
  if $FIXED_TZ; then
    FIXED_TZ_FLAG="--fixed-tz"
  fi
  ${TIMEZONE_SCRIPT} ${COMMON_PARAMS} ${FIXED_TZ_FLAG} ${COUNTRY} ${TIMEZONE}
  echo "Timezone conversion completed."
else
  echo "Step 3: Skipping timezone conversion."
fi

# Step 4: Identify home and work locations
if $RUN_HW; then
  echo "Step 4: Running home/work classification..."
  ${HW_SCRIPT} ${COMMON_PARAMS} ${COUNTRY}
  echo "Home/work classification completed."
else
  echo "Step 4: Skipping home/work classification."
fi

echo "=========================================================="
echo "Pipeline completed successfully!"
echo "Final tables generated:"
echo "  - ${DATASET}.${COUNTRY}${YEAR} (Raw data)"
echo "  - ${DATASET}.${COUNTRY}_stops (Detected stops)"
echo "  - ${DATASET}.${COUNTRY}_clustered_stops (Clustered stops)"
echo "  - ${DATASET}.${COUNTRY}_local (Stops with local time)"
echo "  - ${DATASET}.${COUNTRY}_HW (Home and work classification)"
echo "=========================================================="
