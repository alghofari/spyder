import os
import sys

from google.cloud import storage

from datetime import date
from datetime import timedelta

from os.path import isdir
from os.path import basename

from helpers.time_helper import sleep_condition
from extract.shopee.seller_center.helper.helper import input_password

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains



# def download_profile(storage_client, official_store_name):
#     bucket_name = 'sirclo-data-marketplace'
#     prefix = 'assets/excel/shopee/profile/Tsubaki by FineToday/profile/'
#     dl_dir = 'profile/'

#     storage_client = storage.Client()
#     if isdir(dl_dir) == False:
#         os.makedirs(dl_dir)

#     bucket = storage_client.bucket(bucket_name=bucket_name)
#     blobs = bucket.list_blobs(prefix=prefix)  # Get list of files
#     for blob in blobs:
#         blob_name = blob.name 
#         dst_file_name = blob_name.replace(f'assets/excel/shopee/profile/{official_store_name}/profile/', dl_dir) #.replace('FOLDER1/FOLDER2', 'D:\\my_blob_data') 
        
#         # extract the final directory and create it in the destination path if it does not exist
#         dst_dir = dst_file_name.replace('/' + basename(dst_file_name), '')
#         if isdir(dst_dir) == False:
#             os.makedirs(dst_dir)
#         # download the blob object
#         blob.download_to_filename(dst_file_name)
def get_datacenter(driver, seller_center_pass):
    """
    Login to datacenter dashboard (for traffic and store perf)
    Args:
        driver (any): chromedriver.Driver used
        seller_center_pass (str): seller center password
    Returns:
        None
    """
    driver.get("https://seller.shopee.co.id/datacenter/dashboard")
    
    input_password(driver, seller_center_pass)



def get_traffic_data(driver, storage_client, official_store_name):
    """
    Function to get traffic data by element
    Args:
        driver (any): chromedriver.Driver used
        storage_client (any): storage.Client for sirclo-data-prod
        official_store_name (str): official store name for GCS
    Returns:
        None
    """
    sleep_condition(10,20)
    date_button = driver.find_element(By.XPATH, "//span[ text() = 'Real-time : ']")
    ActionChains(driver).move_to_element(date_button).click(date_button).perform()

    sleep_condition(30,50)
    days_button = driver.find_element(By.XPATH, '//span[ text() = "30 hari sebelumnya." ]')
    ActionChains(driver).move_to_element(days_button).click(days_button).perform()

    sleep_condition(30,50)
    days_button = driver.find_element(By.CSS_SELECTOR, '.track-click-normal-export.shopee-button.shopee-button--normal')
    ActionChains(driver).move_to_element(days_button).click(days_button).perform()

    sleep_condition(100,200)
    driver.save_screenshot(f'{os.getcwd()}/download.png')
    for path, subdirs, files in os.walk(f"{os.getcwd()}"):
        for file in files:
            if file.endswith(".xlsx"):
                path = f"{os.getcwd()}/download_folder/{file}"

                start_date = date.today()
                end_date   = start_date - timedelta(days=30)

                client  = storage_client.get_bucket('sirclo-data-marketplace')
                blob    = client.blob(f'assets/excel/shopee/traffic/{official_store_name}/{date.today()}/Shopee_Traffic_{end_date}_{start_date}.xlsx')
                
                blob.upload_from_filename(path)

                os.remove(path)


def main(driver, storage_client, config):
    official_store_name, vaultwarden_email, seller_center_pass, official_store_id, ned_email = config
    get_datacenter(driver, seller_center_pass)
    get_traffic_data(driver, storage_client, official_store_name)
