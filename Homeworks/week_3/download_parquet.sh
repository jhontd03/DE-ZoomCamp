#!/bin/bash

# Base URL for the files
base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022"

# Loop through months 01 to 12
for month in {01..12}
do
    # Form the complete URL
    url="${base_url}-${month}.parquet"

    # Use wget to download the .parquet file
    wget --no-check-certificate "${url}"
done
