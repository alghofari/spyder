import os
import sys

from datetime import date
from datetime import timedelta

from helpers.time_helper import sleep_condition

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from extract.shopee.seller_center.helper.helper import click_action

def get_non_fulfilled(driver, storage_client, official_store_name):
    """
    Function to get traffic data by element
    Args:
        driver (any): chromedriver.Driver used
        storage_client (any): storage.Client for sirclo-data-prod
        official_store_name (str): official store name for GCS
    Returns:
        None
    """
    sys.stdout.write("Start Non Fulfilled")
    sleep_condition(10,20)
    driver.get("https://seller.shopee.co.id/portal/accounthealth/nfr")

    sleep_condition(30,50)
    driver.save_screenshot(f'{os.getcwd()}/non_fulfilled.png')

    # click_action(driver, css_selector=".tablePanel.tablePanel.tablePanel .tbar .export .shopee-button.shopee-button--normal").click()
    download_non_fulfilled = driver.find_element(By.CSS_SELECTOR, ".tablePanel.tablePanel.tablePanel .tbar .export .shopee-button.shopee-button--normal")
    ActionChains(driver).move_to_element(download_non_fulfilled).click(download_non_fulfilled).perform()

    sleep_condition(100,200)
    driver.save_screenshot(f'{os.getcwd()}/download.png')
    for path, subdirs, files in os.walk(f"{os.getcwd()}"):
        for file in files:
            if file.endswith(".xlsx"):
                path = f"{os.getcwd()}/download_folder/{file}"

                start_date = date.today()
                end_date   = start_date - timedelta(days=30)

                client  = storage_client.get_bucket('sirclo-data-marketplace')
                blob    = client.blob(f'assets/excel/shopee/non_fulfilled/{official_store_name}/{date.today()}/Shopee_Non_Fulfilled_{end_date}_{start_date}.xlsx')
                
                blob.upload_from_filename(path)

                os.remove(path)


def main(driver, storage_client, official_store_name):
    get_non_fulfilled(driver, storage_client, official_store_name)