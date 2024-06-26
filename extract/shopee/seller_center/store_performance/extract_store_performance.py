import os
import sys

from os.path import isdir
from os.path import basename

from helpers.time_helper import sleep_condition

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from google.cloud import storage

from datetime import date, timedelta


def get_store_data(driver, storage_client, official_store_name):
    """
    Function to get store_performance data by element
    Args:
        driver (any): chromedriver.Driver used
        storage_client (any): storage.Client for sirclo-data-prod
        official_store_name (str): official store name for GCS
    Returns:
        None
    """
    sys.stdout.write("Start Store Performance")
    driver.get("https://seller.shopee.co.id/datacenter/products/analysis/overview")

    sleep_condition(30,50)
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
                blob    = client.blob(f'assets/excel/shopee/store_performance/{official_store_name}/{date.today()}/Shopee_Store_Performance_{end_date}_{start_date}.xlsx')
                
                blob.upload_from_filename(path)

                os.remove(path)


def main(driver, storage_client, official_store_name):
    get_store_data(driver, storage_client, official_store_name)
