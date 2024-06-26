import os
import re
import json
import pathlib
import requests
import logging
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
import time
import gspread
import random
from bs4 import BeautifulSoup
from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition
from pyvirtualdisplay import Display
from helpers.bigquery_helper import df_to_bq
from selenium.webdriver.chrome.service import Service
from seleniumwire import webdriver
from seleniumwire.utils import decode
from webdriver_manager.chrome import ChromeDriverManager
from helpers.dataframe_helper import datatype_casting


def set_webdriver(proxy_ip=''):
    my_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f"--user-agent={my_user_agent}")
    if proxy_ip != '':
        chrome_options.add_argument(f"--proxy-server={proxy_ip}")
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


# Loop through the dictionary and identify keys with list values or None
def excluded_columns(d):
    columns = []
    for key, value in d.items():
        if isinstance(value, list) or value is None:
            columns.append(key)
    return columns


def pandas_csv(filename):
    df = pd.read_csv(filename)
    return df


def add_missing_columns(df, schema_path, timestamp):
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
            if column == "load_timestamp":
                df[column] = timestamp
            else:
                df[column] = None

    return df


def main(bucket_name: str, base_path: str, run_date: str, timestamp, table_name: str, schema_path: str):
    # add path for saving json file

    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    # get URL from csv
    df_os = pandas_csv("./extract/tokopedia/official_store/tokopedia_os.csv")
    os_url = list(df_os['url'])
    print(os_url)
    os_name = list(df_os['os'])

    response = requests.get("http://10.103.255.45:8000/indo")
    response_dict = response.json()
    list_proxy = response_dict["address"]

    for i in range(len(os_url)):
        url = os_url[i]
        # set proxy IP
        print(url)
        proxy_ip = random.choice(list_proxy)

        try:
            driver = set_webdriver(proxy_ip)
            driver.get(url)
            sleep_condition(2, 5)

            # Extract JSON data from page source using BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            json_data = soup.get_text()

            # Replace "true" with True, "false" with False, and "null" with None in the JSON string
            json_data = json_data.replace('true', 'True').replace('false', 'False').replace('null', 'None')

            # Parse the JSON string to a Python dictionary
            full_data = eval(json_data)
            products_data = full_data["data"]["products"]

            # Save JSON to GCS
            filename = f"{base_dir}/{os_name[i]}.json"
            blob_name = f"{base_path}/{run_date}/{os_name[i]}.json"
            try:
                dict_to_gcs(products_data, bucket_name, filename, blob_name)
                print("finish add to gcs")
            except Exception as e:
                logging.error(e)
                print("Failed to load to GCS", e)

            # Declare empty DataFrame for all df in list
            df_final = pd.DataFrame()

            # Close the browser window
            driver.quit()

            for j in range(len(products_data)):
                try:
                    data = products_data[j]

                    columns_to_exclude = excluded_columns(data)

                    # Flatten the dictionary using pd.json_normalize
                    df = pd.json_normalize(data, sep='_')

                    # Drop the columns from the DataFrame
                    df.drop(columns=columns_to_exclude, inplace=True)
                    df = add_missing_columns(df, schema_path, timestamp)

                    df_final = pd.concat([df_final, df], ignore_index=True)
                except Exception as e:
                    logging.error(e)
                    print("Failed to parse data")

            try:
                with open(schema_path, 'r') as schema_file:
                    schema = json.load(schema_file)
                casted_dataframe = datatype_casting(schema, df_final)
                df_to_bq(casted_dataframe, table_name, schema_path, "load_timestamp", "search_item")
            except Exception as e:
                print("Failed to load to BQ", e)

        except Exception as e:
            logging.error(e)
            print("Failed to get URL", e)
