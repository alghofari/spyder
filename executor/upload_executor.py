import glob
import os
import re
import time

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.date_helper import get_today


def gcs_upload_bucket(os_name, page_name, marketplace, file_type, gcs_logger):
    # Define config
    bucket = "sirclo-data-marketplace"

    # Define format file download
    if file_type == "excel":
        format_file = "xlsx"
    elif file_type == "json":
        format_file = "json"

    # Get filename from directory upload
    current_directory = os.getcwd()
    filepath = f"{current_directory}/assets/{file_type}/{marketplace}/{page_name}/{os_name}"
    filename = glob.glob(filepath + f'/*.{format_file}')
    fix_filename = filename[0]

    # Clean filename
    if format_file == "xlsx":
        output_filename = re.findall(r'[\\\/]([^\\\/]+)$', fix_filename)[0].split('.xlsx')[0]
    
    elif format_file == "json":
        output_filename = re.findall(r'[\\\/]([^\\\/]+)$', fix_filename)[0].split('.json')[0]
    
    else:
        gcs_logger.log(f"There is no mapping for file {fix_filename} with format {format_file}")
        raise ValueError(f"The page {page_name} is not trackable, please make sure the download file output")

    # Define blob name
    blob_name = f'assets/{file_type}/{marketplace}/{page_name}/{os_name}/{get_today()}/{output_filename}.{format_file}'

    if os_name != '':
        if os.path.exists(fix_filename):
            upload_blob_to_gcs(bucket, fix_filename, blob_name)
            gcs_logger.log(
                f"Successfully upload file to GCS Bucket : {bucket}/{blob_name}")
            time.sleep(5)
