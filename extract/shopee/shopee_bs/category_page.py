import os
import pathlib
import time
import random
import logging
import pandas as pd
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

from helpers.cloud_storage_helper import upload_blob_to_gcs, list_blob_gcs, download_blob_to_local
from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import connect_to_oxylab, connect_to_vpn


def get_data(driver, category: str, base_dir: str, bucket_name: str, base_path: str, run_date: str):
    driver.get(f"https://shopee.co.id/{category}")
    time.sleep(5)

    for i in range(10):
        driver.execute_script("window.scrollBy(0,500)")

    driver.execute_script("window.scrollBy(0,50)")
    sleep_condition(7,12)

    html = driver.page_source
    html_filename = f"{base_dir}/{category}_page_1.html"
    with open(html_filename, "w") as f:
        f.write(html)

    blob_name = f"{base_path}/{run_date}/{category}_page_1.html"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=html_filename, gcs_blob_name=blob_name)

    print(f"successfully write file to {blob_name}")

    # Find multiple elements using XPath
    item_elements = driver.find_elements(By.XPATH, "(//div[@class='DgXDzJ rolr6k Zvjf4O'][contains(text(),'\')])")
    print("checking item_elements.....")
    print(item_elements)

    # Iterate through the found elements and do something with them
    for element in item_elements:
        print(element.text)

def choose_vpn(bucket_name): 
     #list vpn profile file in gcs
    vpn_profile_list = list_blob_gcs(bucket_name=bucket_name, prefix_name="assets/ovpn/")
    vpn_profile_list.remove('assets/ovpn/')
    picked_profile = random.choice(vpn_profile_list)
    print(f"the profile used is {picked_profile}")

     # download vpn profile from cloud storage
    vpn_filename = picked_profile.split('/')[-1]
    download_blob_to_local(bucket_name=bucket_name, local_file_name=vpn_filename, gcs_blob_name=picked_profile)
    print("finish download profile vpn")
    time.sleep(5)
    return vpn_filename


def set_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    
    # proxy_ip = connect_to_oxylab()
    # chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def get_categories():
    read_csv = pd.read_csv("./extract/shopee/shopee_item/Shopee_category.csv")
    category = read_csv['Category Name']
    return list(category)


def main(bucket_name: str, base_path: str, run_date: str, sort_by: str):
    driver = set_webdriver()

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    vpn_used = choose_vpn(bucket_name=bucket_name)
    vpn_use = "yes"

    if vpn_use == "yes":
        connect_to_vpn(vpn_used)
    sleep_condition(10, 20)

    categories = get_categories()
    random.shuffle(categories)

    for category in categories:
        try:
            get_data(driver, category, base_dir, bucket_name, base_path, run_date)
        except Exception as e:
            print(e)
            print("html file is not sufficient")
            continue

    driver.quit()
    logging.info("Closing virtual display")