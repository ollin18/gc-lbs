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
  -hd 2 \
  -wd 2 \
  -he 19 \
  -hm 8 \
  -ws 9 \
  -we 18 \
  -md 50 \
  CO

# Can run help with --help flag
bash get_hw.sh --help
bash run_pipeline.sh --help

bash dbscan.sh \
  -p "job-accessibility" \
  -d "lbs_latam" \
  -i "lbs-laltam" \
  -o "lbs_laltam" \
  -dd 20 \
  -mp 2 \
  CO

