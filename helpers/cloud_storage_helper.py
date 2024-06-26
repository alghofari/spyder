from google.cloud import storage
import json
import logging
import os


def upload_blob_to_gcs(bucket_name, local_file_name, gcs_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_file_name)
    return

def download_blob_to_local(bucket_name, local_file_name, gcs_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.download_to_filename(local_file_name)
    return

def list_blob_gcs(bucket_name, prefix_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix_name)
    list_blob = [blob.name for blob in blobs]
    return list_blob

def download_folder_from_gcs(bucket_name, gcs_path, destination_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_path)

    for blob in blobs:
        if not blob.name.endswith('/'):  # Exclude subdirectories
            # Create the destination directory if it doesn't exist
            end_blob = blob.name.split(gcs_path)[-1]
            destination_directory = f"{destination_path}/{end_blob}"

            # Create the directory if it doesn't exist
            need_path = '/'.join(destination_directory.split('/')[:-1])
            if not os.path.exists(need_path):
                os.makedirs(need_path)
            
            # Download the file to the destination path
            blob.download_to_filename(destination_directory)
            print(f"Downloaded {blob.name} to {destination_directory}")


def detect_file(bucket_name, prefix_path_name, number_of_bucket):
    # Hook the GCS Connection
    storage_client = storage.Client()

    # Get the blobs list
    blobs_list = storage_client.list_blobs(bucket_or_name=bucket_name,
                                            prefix = prefix_path_name)

    # Count all the product
    total_file = sum(1 for _ in blobs_list)
    if total_file > number_of_bucket:
        print(f"File has already in the bucket")
        return True

    else:
        print("File has not been uploaded, there might be error with your pipeline")
        return False

def load_gcs_json(bucket_name: str, blob_name: str, client=None, debug=None): 

    if client == None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
    else:
        bucket = client.bucket(bucket_name)

    # Get a reference to the desired JSON file on GCS
    blob = bucket.blob(blob_name)

    # Read the file contents as a stream
    json_data = json.loads(blob.download_as_string())
    if debug == None:
        print(json_data)
    
    return json_data

def load_gcs_string(bucket_name: str, blob_name: str, client=None): 

    if client == None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
    else:
        bucket = client.bucket(bucket_name)

    # Get a reference to the desired JSON file on GCS
    blob = bucket.blob(blob_name)

    # Read the file contents as a stream
    json_data = json.loads(blob.download_as_text())
    return json_data

def enhanced_metadata_blob_gcs(bucket_name: str, prefix_name: str):
    blob_metadata = {}
    storage_client    = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix_name)

    # Check if there are any blobs
    if not blobs:
        raise Exception("No files found in the specified location.")

    # Create dictionary if blobs exist
    for blob in blobs:
        blob_metadata[blob.name] = {
            "upload_tstamp": blob.updated,
            "excel_filename": blob.name.split("/")[-1],
            "store_foldername": blob.name.split("/")[4],
            "target_filepath": f"gs://{bucket_name}/{blob.name}"
        }
        
    return blob_metadata

def enhanced_gcs_json(bucket_name: str, prefix_name: str): 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix_name) # List blobs with the specified prefix

    # Check if there are any blobs
    if not blobs:
        raise Exception("No files found in the specified location.")

    for blob in blobs:
        json_data = json.loads(blob.download_as_text())

    return json_data