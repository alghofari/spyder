# https://pages.lazada.co.id/wow/i/id/LandingPage/lazmall

import json
import logging
import os
import pathlib
import random
import time
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

import gspread
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager


def set_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

def show_more_button(): 
    while True:
        try: 
            show_button = driver.find_element("xpath", "(//a[@class='button J_LoadMoreButton'][normalize-space()='Memuat Lebih Banyak'])[1]")
            show_button.click()
            print("menampilkan lebih banyak shop")
            time.sleep(5)
            # extract()
        except: 
            print("sudah tidak ada lagi")
            extract()
            break

def extract(): 
    request_url = "?jsv=2.7"


    for request in driver.requests:
        if request.response:
            if request.url.find(request_url) > 0:
                logging.info(request.url, request.response.status_code, request.response.headers['Content-Type'])
                body = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                json_str = body.decode('utf-8')
                # print(type(json_str))
                # print("masih string")
                try: 
                    parsed_data = json.loads(json_str)
                    print("bisa 1")
                    for i in parsed_data['data']['resultValue']['20180501004']['data']: 
                        print(i['shopName'])
                        shop_url = i['shopUrl'].lstrip('/')
                        print("bisa 2")
                        print(shop_url)
                        shop_names.append(i['shopName'])
                        shop_urls.append(shop_url)
                except Exception as e: 
                    print(e)
                    print("gak bisa di parse")


def load_to_bq(df, target_table): 
    job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND")

    # Construct a BigQuery client object.
    client = bigquery.Client()

    job = client.load_table_from_dataframe(
            df, target_table, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.


shop_names = []
shop_urls = []

driver = set_webdriver()

url = "https://pages.lazada.co.id/wow/i/id/LandingPage/lazmall"
driver.get(url)
print("cekpoint 1")
time.sleep(3)

# for button in list_button: 
for i in range(2,9):
    try : 
        if i == 7: 
            continue

        x_button = driver.find_element("xpath", f"(//div[contains(@class,'category-tab-image')])[{i}]")
        x_button.click()
        time.sleep(10)
        
        # extract()
        
        print("Cekpoin 1")
        show_more_button()
        print("==========")
    
    except: 
        print(f"not getting the button {i}")

driver.quit()

# Create a dictionary from the lists
data = {'ShopName': shop_names, 'ShopURL': shop_urls}

# Create a DataFrame from the dictionary
df = pd.DataFrame(data)
df_no_duplicates = df.drop_duplicates(keep='last')
print(df_no_duplicates.info())

# load_to_bq(df_no_duplicates, "sirclo-prod.bronze_marketplace.lazada_mall_os_list")
# print("finish load to bq")

