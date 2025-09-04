#!/usr/bin/env bash

# Complete Pipeline
bash run_pipeline.sh CO 2024 America/Bogota

# Skipping steps
bash run_pipeline.sh --skip-stops --skip-dbscan --skip-timezone CO 2024 America/Bogota

# Run only specific steps
# Doing this won't read the config file, so you need to pass all required
# parameters
bash get_hw.sh \
  -p "job-accessibility" \
  -d "lbs_latam" \
  -i "lbs-laltam" \
  -o "lbs_laltam" \
  -hd 10 \
  -wd 5 \
  -he 20 \
  -hm 7 \
  -ws 8 \
  -we 19 \
  -md 100 \
  CO

# Can run help with --help flag
bash get_hw.sh --help
bash run_pipeline.sh --help

# Run stays for each year and then the rest of the pipeline
bash run_pipeline.sh --skip-dbscan --skip-timezone --skip-hw AR 2023 America/Buenos_Aires
bash run_pipeline.sh --skip-dbscan --skip-timezone --skip-hw AR 2024 America/Buenos_Aires
bash run_pipeline.sh --skip-stops AR 2024 America/Buenos_Aires
bash run_pipeline.sh --skip-stops --skip-dbscan --skip-timezone AR 2024 America/Buenos_Aires
