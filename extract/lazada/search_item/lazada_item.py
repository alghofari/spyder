import os
import re
import json
import pathlib
import requests
import logging
import pandas as pd
import urllib.request
from urllib.parse import urlparse, parse_qs, unquote
import time
import gspread
import random
from bs4 import BeautifulSoup
from helpers.cloud_storage_helper import upload_blob_to_gcs, download_blob_to_local, list_blob_gcs
from helpers.time_helper import sleep_condition
from pyvirtualdisplay import Display
from helpers.bigquery_helper import df_to_bq
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager
from helpers.webdriver_helper import connect_to_vpn, connect_to_oxylab


def get_csv_column(filename, column_name):
    import csv
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        column = [row[column_name] for row in reader]
    return column


def pandas_csv(filename):
    df = pd.read_csv(filename)
    return df


def set_webdriver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    # if list_proxy:
    #     proxy_ip = random.choice(list_proxy)
    #     chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


def dict_to_gcs(data: dict, bucket_name: str, filename: str, blob_name: str):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=filename, gcs_blob_name=blob_name)
    print("successfully upload to gcs")
    return


def parsing_json(data: dict):
    new_df = pd.json_normalize(data)
    column_names = list(new_df.columns.values)
    column_names = [text.replace('__', '').replace('.', '__').lower() for text in column_names]
    new_df.columns = column_names
    return new_df


def add_missing_columns(df, schema_path, category, timestamp):
    # Iterate over JSON schema files in the directory
    with open(schema_path, "r") as file:
        schema = json.load(file)

        # Convert the JSON schema to a list of column names
        columns_in_schema = [column["name"] for column in schema]

        # Get the list of columns in the DataFrame
        columns_in_df = list(df.columns)

        # Find columns in the DataFrame that are not in the schema
        extra_columns = set(columns_in_df) - set(columns_in_schema)

        # Remove extra columns from the DataFrame
        df.drop(extra_columns, axis=1, inplace=True)

        # Add missing columns to the DataFrame with None as the initial value
        missing_columns = set(columns_in_schema) - set(columns_in_df)
        for column in missing_columns:
            if column == "category_name":
                df[column] = category
            elif column == "load_timestamp":
                df[column] = timestamp
            else:
                df[column] = None

    return df

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


def get_api(url: str, base_dir: str, category: str, bucket_name: str, base_path: str, run_date: str, page: int, oxylab_use: str): 
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    if oxylab_use != "no": 
        proxies = connect_to_oxylab()
        response = requests.get(url, headers=headers, proxies=proxies) 
    else: 
        response = requests.get(url, headers=headers)
        print("not using oxylab")

    if response.status_code == 200:
        time.sleep(3)
        json_data = response.json()

    else:
        print("Failed to fetch JSON data:", response.status_code)
        pass

    # Convert the dictionary to a JSON-formatted string
    json_string = json.dumps(json_data)
    # Parse the JSON string into a Python dictionary
    data = json.loads(json_string)


    # body = json.loads(body)
    all_items = data["mods"]["listItems"]
    breadcrumb = data["mods"]["breadcrumb"]
    filename = f"{base_dir}/{category}_{page}.json"
    blob_name = f"{base_path}/{run_date}/{category}_{page}.json"
    dict_to_gcs(all_items, bucket_name, filename, blob_name)
    return all_items, breadcrumb, blob_name

def main(start: int, end: int, category: str, bucket_name: str, base_path: str, run_date: str, timestamp,
         table_name: str, schema_path: str, vpn_use="no", oxylab_use="no"):
   
    # add path for saving json file
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    # read list category from csv file
    df_category = pandas_csv("./extract/lazada/search_item/lazada_categories.csv")
    categories_url = list(df_category['Link'])
    random.shuffle(categories_url)

    if vpn_use == "yes": 
        vpn_used = choose_vpn(bucket_name=bucket_name)
        connect_to_vpn(vpn_used)

    # loop by category
    for index, base_url in enumerate(categories_url):
        category = df_category[df_category['Link'] == base_url]['Category'].iloc[0]
        print(category)

        pages = list(range(start, end))
        random.shuffle(pages)

        # loop by page
        for page in pages:
            url = base_url + str(page) + '&spm=a2o4j'
            print(url)

            # do try mechanism to hit API with oxylab
            max_retries = 10
            retry = 0
            # body = None\
            while retry < max_retries:
                try:
                    all_items, breadcrumb, blob_name = get_api(url, base_dir, category, 
                                                            bucket_name, base_path, run_date, page, oxylab_use)

                    # set variable for dataframe
                    df_final = pd.DataFrame()
                    break

                except Exception as e:
                    logging.error(e)
                    retry += 1
                    if retry < max_retries:
                        print(f"retry attempt {retry}/{max_retries} for {category} at page {page}")
                    else: 
                        print(f"Category {category} at page {page} won't load")
                    sleep_condition(3, 6)

            # transform it so can be loaded into BQ
            for j in range(len(all_items)):
                item = all_items[j]
                try:
                    category_order = json.dumps(breadcrumb)
                    df = parsing_json(item)
                    excluded_columns = ["skus", "icons", "categories", "thumbs"]
                    df = df.drop(excluded_columns, axis=1)
                    df = add_missing_columns(df, schema_path, category_order, timestamp)
                    df_final = pd.concat([df_final, df], ignore_index=True)
                except Exception as e:
                    logging.error(e)
                    print(f"Parsing Failed on {blob_name}")
                    continue
            try:
                df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
            except Exception as e:
                logging.error(e)
                print("wrong schema for category ", category, e)
                continue
            print(f"Data has been inserted to {table_name}")
            sleep_condition(5, 30)
        sleep_condition(5, 30)