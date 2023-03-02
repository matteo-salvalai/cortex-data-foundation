#!/bin/bash
# Create test tables from GCS bucket
bucket="$1"
project="$2"
dataset="$3"
location=${4:-US}
location_low=$(echo "${location}" | tr '[:upper:]' '[:lower:]')

valid_locations="us-central1 us-west4 us-west2 northamerica-northeast1 northamerica-northeast2 us-east4 us-west1 us-west3 southamerica-east1 southamerica-west1 us-east1 asia-south2 asia-east2 asia-southeast2 australia-southeast2 asia-south1 asia-northeast2 asia-northeast3 asia-southeast1 australia-southeast1 asia-east1 asia-northeast1 europe-west1 europe-north1 europe-west3 europe-west2 europe-west4 europe-central2 europe-west6"

if [[ "${valid_locations}" =~ "${location_low}" ]]; then
  echo "Loading parquet in location ${location_low}"
else
  echo "ERROR: Location ${location_low} is not a valid location for the test harness."
  echo "Please set _TEST_DATA to false or use a supported location listed in README. Check load_parquet.sh:valid_locations"
  exit 1
fi

if [[ "${location_low}" == 'australia-southeast1' ]]; then
    location_low=australia-southeast11
fi

# Temporary URL list file.
url_list_file=$(mktemp)

# Getting a list of URLs.
gsutil ls "${bucket}" >> "${url_list_file}"
#gcs://{bucket}/replication/*/delta/
#replication/*/initial/

# Disable existing on error because need to delete the temp file
set +e

# Loading to BigQuery.
python3 src/utils/bqload.py --source-list-file "${url_list_file}" --dataset "${project}.${dataset}" \
  --location "${location}" --parallel-jobs 10
ERR_CODE=$?

# Enable exiting on error back.
set -e

# Deleting temporary file.
rm -f "${url_list_file}"

exit $ERR_CODE
