from google.cloud import bigquery, storage
import os



from google.cloud import bigquery
from google.cloud import storage

def load_csv_files_to_bigquery(bucket_name, dataset_id):
    if not bucket_name or not dataset_id:
        raise ValueError("Bucket name and dataset ID must be provided")

    # Initialize BigQuery client and GCS client in order to connect to GCP 
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()

    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        
        for blob in blobs:
            
            # Define the URI for the CSV file in the bucket
            uri = f'gs://{bucket_name}/{blob.name}'
            
            table_ref = bigquery_client.dataset(dataset_id).table(blob.name)
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip the header row
                autodetect=True,  # Automatically detect the schema
            )
            
            # Load the CSV file into BigQuery
            load_job = bigquery_client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )

            load_job.result()
            print(f'Loaded {blob.name} into {dataset_id}.{blob.name}')
    except Exception as e:
        print(f"Error loading files {e}")
        raise

if __name__ == '__main__':
    # Define name of dataset and bucket name created in GCP project
    bucket_name = 'runna'
    dataset_id = 'runna'
    
    load_csv_files_to_bigquery(bucket_name, dataset_id)


