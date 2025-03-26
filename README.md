# LBS Data Processing Pipeline

This repository contains a production-ready pipeline for processing location-based services (LBS) data to identify stops, clusters, and home/work locations.

## Overview

The pipeline consists of four main steps:

1. **Stops Detection**: Converts raw location pings into meaningful stops.
2. **Spatial Clustering**: Groups nearby stops into clusters using DBSCAN algorithm.
3. **Timezone Conversion**: Converts timestamps to local time.
4. **Home/Work Classification**: Identifies home and work locations for each user.

## Requirements

- Google Cloud Platform account with BigQuery access
- GCS bucket with raw LBS data organized by country/year
- BigQuery dataset to store the processed data
- Bash environment with `bq` command-line tool installed

## Directory Structure

```
pipeline/
├── run_pipeline.sh             # Main orchestration script
├── pipeline_config.json        # Configuration file
├── process_yearly_stays.sh     # Step 1: Stops detection
├── dbscan.sh                   # Step 2: Spatial clustering
├── local_time.sh               # Step 3: Timezone conversion
├── get_hw.sh                   # Step 4: Home/work classification
└── README.md                   # This file
```

## Usage

### Quick Start

To run the full pipeline for a country and year:

```bash
./run_pipeline.sh CO 2022 America/Bogota
```

This will process Colombia's data for 2022 using Bogota timezone as default.

### Advanced Usage

You can customize the pipeline with various command-line options:

```bash
./run_pipeline.sh \
  --project my-gcp-project \
  --dataset my_dataset \
  --input-bucket my-input-bucket \
  --output-bucket my-output-bucket \
  CO 2022 America/Bogota
```

### Running Specific Steps

You can skip any steps you don't need:

```bash
./run_pipeline.sh \
  --skip-stops \        # If stops are already processed
  --skip-dbscan \       # If clustering is already done
  CO 2022 America/Bogota
```

### Running Individual Scripts

Each step can be run independently:

```bash
# Process stops
./process_yearly_stays.sh \
  --project my-gcp-project \
  --dataset my_dataset \
  --distance 25 \         # Custom distance threshold (meters)
  --time 1800 \           # Custom time threshold (seconds)
  --stop-duration 600000 \ # Custom minimum stop duration (ms)
  CO 2022

# Run DBSCAN clustering
./dbscan.sh \
  --dbscan-dist 30 \      # Custom DBSCAN distance (meters)
  --min-points 3 \        # Custom minimum points per cluster
  CO

# Convert to local timezone
./local_time.sh \
  CO America/Bogota

# Identify home and work locations
./get_hw.sh \
  --home-days 10 \        # Custom minimum days for home
  --work-days 5 \         # Custom minimum days for work
  --home-evening 20 \     # Evening starts at 8PM
  --home-morning 7 \      # Morning ends at 7AM
  --work-start 8 \        # Work day starts at 8AM
  --work-end 19 \         # Work day ends at 7PM
  --min-distance 100 \    # Minimum home-work distance (meters)
  CO
```

## Configuration

You can edit `pipeline_config.json` to set default values for all parameters by country:

```json
{
  "project": "job-accessibility",
  "dataset": "lbs_latam",
  "countries": {
    "CO": {
      "timezone": "America/Bogota",
      "stops": {
        "distance_threshold": 20,
        "time_threshold": 3600,
        "min_stop_duration": 300000
      },
      ...
    }
  }
}
```

## Output Tables

The pipeline produces the following BigQuery tables:

1. `{dataset}.{country}{year}` - Raw data
2. `{dataset}.{country}_stops` - Detected stops
3. `{dataset}.{country}_clustered_stops` - Clustered stops
4. `{dataset}.{country}_local` - Stops with local time
5. `{dataset}.{country}_HW` - Home and work classification

## Final Table Schema

The final `{country}_HW` table has the following schema:

| Column Name        | Data Type | Description                                                   |
|--------------------|-----------|---------------------------------------------------------------|
| uid                | STRING    | Unique device identifier                                      |
| stop_event         | INTEGER   | Sequential numbering of stops for each device                 |
| start_timestamp    | INTEGER   | Start time of the stop in Unix seconds                        |
| end_timestamp      | INTEGER   | End time of the stop in Unix seconds                          |
| stop_duration      | INTEGER   | Duration of the stop in seconds                               |
| stop_latitude      | FLOAT     | Latitude of the stop location                                 |
| stop_longitude     | FLOAT     | Longitude of the stop location                                |
| cluster_label      | INTEGER   | Spatial cluster ID (-1 for non-clustered stops)               |
| cluster_counts     | INTEGER   | Number of stops in the cluster                                |
| cluster_latitude   | FLOAT     | Latitude of the cluster centroid                              |
| cluster_longitude  | FLOAT     | Longitude of the cluster centroid                             |
| timezone           | STRING    | Timezone identifier derived from coordinates                  |
| inputed_timezone   | STRING    | Timezone used (falls back to default if timezone is NULL)     |
| stop_datetime      | TIMESTAMP | Start time in local timezone                                  |
| end_stop_datetime  | TIMESTAMP | End time in local timezone                                    |
| date               | DATE      | Date of the stop start (local time)                           |
| year               | INTEGER   | Year of the stop                                              |
| month              | INTEGER   | Month of the stop (1-12)                                      |
| day                | INTEGER   | Day of the month                                              |
| hour               | INTEGER   | Hour of the day (0-23)                                        |
| weekday            | INTEGER   | Day of week (1=Sunday, 7=Saturday)                            |
| weekend            | BOOLEAN   | TRUE if stop is on a weekend (Saturday or Sunday)             |
| end_date           | DATE      | Date of the stop end (local time)                             |
| location_type      | STRING    | Location classification: 'H' (Home), 'W' (Work), 'O' (Other)  |
| location_label     | INTEGER   | Numeric label for tracking location changes across years      |

## Key Parameters

### Stops Detection

- **Distance Threshold**: Maximum distance (meters) between consecutive pings to be considered the same stop (default: 20m)
- **Time Threshold**: Maximum time gap (seconds) between consecutive pings to be considered the same stop (default: 3600s)
- **Minimum Stop Duration**: Minimum duration (milliseconds) required for a valid stop (default: 300000ms = 5 minutes)

### Spatial Clustering

- **DBSCAN Distance**: Maximum distance (meters) between points to be considered part of the same cluster (default: 20m)
- **Minimum Points**: Minimum number of points required to form a cluster (default: 2)

### Home/Work Classification

- **Home Time**: Defined as weekends (all day) and weeknights (default: 7PM-8AM)
- **Work Time**: Defined as weekdays during work hours (default: 9AM-6PM)
- **Home Minimum Days**: Minimum number of unique days visited to qualify as home (default: 14 days)
- **Work Minimum Days**: Minimum number of unique workdays visited to qualify as work (default: 8 days)
- **Minimum Home-Work Distance**: Minimum distance between home and work locations (default: 50 meters)

## Extracting Home and Work Locations

To extract just the home and work locations for each user:

```sql
-- Get distinct home and work locations by user and year
WITH
-- Get home locations
home_locations AS (
  SELECT DISTINCT
    uid,
    year,
    cluster_latitude AS home_latitude,
    cluster_longitude AS home_longitude
  FROM `job-accessibility.lbs_latam.CO_HW`
  WHERE location_type = 'H'
),

-- Get work locations
work_locations AS (
  SELECT DISTINCT
    uid,
    year,
    cluster_latitude AS work_latitude,
    cluster_longitude AS work_longitude
  FROM `job-accessibility.lbs_latam.CO_HW`
  WHERE location_type = 'W'
)

-- Join home and work locations
SELECT
  h.uid,
  h.home_latitude,
  h.home_longitude,
  w.work_latitude,
  w.work_longitude,
  h.year
FROM home_locations h
LEFT JOIN work_locations w
  ON h.uid = w.uid AND h.year = w.year
-- Only return users with a home location
WHERE h.uid IS NOT NULL
ORDER BY h.uid, h.year;
```

## Troubleshooting

- **Missing Home/Work Locations**: Adjust the minimum days thresholds if users aren't getting home/work locations assigned.
- **Incorrect Clustering**: Modify DBSCAN parameters if stops aren't being grouped properly.
- **Timezone Issues**: Check that the timezone lookup table has complete coverage for your country.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
