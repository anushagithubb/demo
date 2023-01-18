# ETL pipeline python code to move the data from MYSQL database ---> Google Cloud Storage --->  Big Query
#import needed modules
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage


proj = 'dynamic-heading-355605'

dataset = 'my_file'
table = 'age_income'
table_id = f'{proj}.{dataset}.{table}'

bucket_name='cloud-12'
destination_blob_name='my-folder/temp-folder'
# data connections
#client = bigquery.Client(project=proj)
load_file = 'D:/ANUSHA/files/age_income.csv'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(destination_blob_name)

blob.upload_from_filename(load_file)
print(load_file)

print('File {} uploaded to {}.'.format(
      load_file,
      destination_blob_name))
      

client = bigquery.Client(project=proj)


job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

uri = "gs://cloud-12/my-folder/temp-folder"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  
# Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))





   