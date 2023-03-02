import argparse

from google.cloud import storage
from google.cloud import bigquery

# Define the bucket and folder paths
project_id = 'fer-ssbi-core-test-prj'
bucket_name = 'fer-ssbi-core-test-data-intelligence'
folder_path = 'replication/'
dataset_id = 'cortex_raw'

# Create a Google Cloud Storage client
storage_client = storage.Client()

# Create a BigQuery client
bigquery_client = bigquery.Client()

# Get a reference to the Google Cloud Storage bucket
bucket = storage_client.bucket(bucket_name)
blobs = bucket.list_blobs(prefix=folder_path)

subfolders = set()
for blob in blobs:
    if(len(blob.name.split('/')) == 3):
        subfolders.add(blob.name.split('/')[1])

# Loop through all folders in the bucket
for folder_name in subfolders:
    # Create a BigQuery external table for the folder
    table_id = f'{project_id}.{dataset_id}.{folder_name.lower()}'
    table_uri = f'gs://{bucket_name}/replication/{folder_name}/*.parquet'

    table = bigquery.Table(table_id)
    external_config = bigquery.ExternalConfig.from_api_repr({
        "sourceFormat": "PARQUET",
        "autodetect": True,
        "source_uris": [
            table_uri
        ]
    })

    table.external_data_configuration = external_config

    bigquery_client.create_table(table)
    print(f'Created external table {table_id} with uri {table_uri}')