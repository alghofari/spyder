import json
import logging
import os
import pathlib
import random
from datetime import datetime
import requests

import gspread
import pandas as pd
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition
from helpers.bigquery_helper import df_to_bq


# Function to setup webdriver chrome config
def set_webdriver(proxy_ip=''):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    if proxy_ip != '':
        chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

# Function to read csv file
def pandas_csv(filename):
    df = pd.read_csv(filename)
    return df

# Function to read dictionary and upload it to gcs
def dict_to_gcs(data: dict, bucket_name: str, filename: str, blob_name: str):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=filename, gcs_blob_name=blob_name)
    print("successfully upload to gcs")
    return

# Function to parsing json and convert it into pandas dataframe
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


def handle_nonrecord_type(data, schema): 
    for field in schema:
        if field['type'] != 'RECORD' and field['name'] not in data.columns:  
            column = field['name']
            data[column] = None
    return data


def get_subcolumn(data,column, schema): 
    nested_field_name = []
    for field in schema:
        if field['name'] == column and field['type'] == 'RECORD': 
            for nested_field in field['fields']:
                nested_field_name.append(nested_field['name'])
    return nested_field_name


def handle_record_type(data, schema): 
    for field in schema: 
        if field['type'] == 'RECORD' and field['name'] not in data.columns: 
            subcolumn = get_subcolumn(data=data,column=field['name'], schema=schema)
            field_dict = {field: None for field in subcolumn}
            column = field['name']
            data[column] = field_dict
    return data

# Main Function
def main(start: int, end: int, bucket_name: str, base_path: str, run_date: str,
         table_name: str, schema_path: str):
    
    # check if directory exist, if not will create directory
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

     # read link category from csv file
    df_category = pandas_csv("./extract/bukalapak/search_item/list_categories.csv")
    category_link = list(df_category['url'])
    random.shuffle(category_link)
    
    # open page and intercept the API
    for page in range(start, end): 
        driver = set_webdriver()
        for link in category_link: 
            body = None 
            category = df_category[df_category['url'] == link]['Category'].iloc[0]
            try: 
                driver.get(f"{link}?page={str(page)}")
                sleep_condition(2, 5)
                driver.find_element(By.CSS_SELECTOR, ".bl-dropdown.te-product-sorting.mr-16.bl-dropdown--has-value").click()
                sleep_condition(2, 5)
                driver.find_element(By.XPATH, "(//p[@class='bl-text bl-text--medium bl-text--ellipsis__1'][normalize-space()='Terlaris'])[2]").click()
                sleep_condition(15, 25)
                request_url = "multistrategy-products"

                for request in driver.requests: 
                    if request.response: 
                        if request.url.find(request_url) > 0: 
                            logging.info(request.url, request.response.status_code,
                                         request.response.headers['Content-Type'])
                            body = decode(request.response.body,
                                          request.response.headers.get('Content-Encoding', 'identity'))
                
                # processing the API result and add to GCS
                if body is not None: 
                    body = json.loads(body)
                    products = body["data"]
                    # print(products)
                    filename = f"{base_dir}/{category}_{page}.json"
                    blob_name = f"{base_path}/{run_date}/{category}_{page}.json"
                    dict_to_gcs(products, bucket_name, filename, blob_name)
                    print(f"finish add to gcs for {category} at page {page}")
                    # print(len(products))
                else:
                    print(f"body doesn't exist for {category} at page {page}")
                    continue
            
            except Exception as e:
                logging.error(e)
                print(f"Category {category} page {page} won't load")
                continue
            
            # Load few json means one items into a dataframe containing all items in a page
            df_final = pd.DataFrame()
            excluded_columns = ["deal__applied_date", "default_catalog__id", "couriers", "wholesales", 
                                "specs__brand", "specs__kapasitas", "installments", "state_description", 
                                "deal__discount_price", "deal__expired_date", "deal__original_price", "deal__percentage", 
                                "discount_subsidy__amount", "discount_subsidy__applied_date", "discount_subsidy__expired_date",
                                "discount_subsidy__max_purchase", "discount_subsidy__stock", "specs__halal", "specs__tipe", 
                                "specs__display_size", "specs__type", "specs__lebar", "specs__merek", "specs__kapasitas_hardisk"
                                "specs__ukuran", "specs__seri", "specs__display_type", "specs__cleaner-type", "specs__diameter", 
                                "specs__processor", "specs__display", "specs__focus_type", "specs__bahan", "specs__bcd", 
                                "specs__drumset", "specs__jenis", "specs__size_shock", "specs__platform", "specs__filter-feature", 
                                "specs__merchant-guarantee", "specs__merchant_guarantee", "specs__features", "specs__garansi", 
                                "specs__capacity", "specs__exhaust-material"]
            
            for j in range(len(products)): 
                item = products[j]
                try: 
                    df = parsing_json(item)
                    df.insert(0, "load_timestamp", datetime.now())


                    # Drop columns if they exist in the DataFrame
                    columns_to_drop = [col for col in excluded_columns if col in df.columns]
                    df = df.drop(columns_to_drop, axis=1)

                    with open(schema_path) as f:
                        schema = json.load(f)
                    #handling non record data
                    df = handle_nonrecord_type(data=df, schema=schema)
                    df = handle_record_type(data=df, schema=schema)
                    
                    df_final = pd.concat([df_final, df], ignore_index=True)

                except Exception as e:
                    logging.error(e)
                    print("Parsing failed ", category, e)


            # Load to bigquery
            print("finish parsing")
            try: 
                df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
                print(f"{category} at page {page} has been inserted to {table_name}")
            except Exception as e:
                logging.error(e)
                print("wrong schema for category ", category, e)
                continue

            sleep_condition(10, 20)
        driver.quit()
