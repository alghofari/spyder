# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 16:04:03 2022

@author: SIRCLO
"""
import os
import pathlib
import time
import pandas as pd
import numpy as np
import random
from bs4 import BeautifulSoup
from helpers.cloud_storage_helper import upload_blob_to_gcs, list_blob_gcs, download_blob_to_local
from helpers.webdriver_helper import set_webdriver, connect_to_vpn
from helpers.bigquery_helper import df_to_bq
from transform.tokopedia.category.category_page_dictionary import Category


def get_category(category: str, level=1):
    return category.rsplit('/', 2)[-level]

def get_csv_column(filename, column_name):
    import csv
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        column = [row[column_name] for row in reader]
    return column

def get_data(page: int, category: str, base_dir: str, bucket_name: str, base_path: str, run_date: str, sort_by: str):
    opts = ['--headless']
    driver = set_webdriver(additional_opts=opts, driver_type="webdriver")
    driver.get(f"https://tokopedia.com/p/{category}?page={page}&ob={sort_by}")
    time.sleep(5)
    category = get_category(category)
    for i in range(20):
        driver.execute_script("window.scrollBy(0,250)")

    driver.execute_script("window.scrollBy(0,50)")

    html = driver.page_source
    html_filename = f"{base_dir}/{category}_page_{str(page)}.html"
    with open(html_filename, "w", encoding="utf-8") as f:
        f.write(html)

    blob_name = f"{base_path}/{run_date}/{category}_page_{str(page)}.html"
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=html_filename, gcs_blob_name=blob_name)

    print(f"successfully write {html_filename}")
    print("successfully upload to gcs")
    driver.quit()

def items_to_df(html, run_date):
    c = Category()
    print("Parsing items from HTML..")
    all_items = html.findAll('div', {'class': 'css-bk6tzz e1nlzfl2'})
    for item_html in all_items:
        for item in c.html_code_page_item:
            args = [item_html]
            args.extend(c.html_code_page_item[item])
            try:
                value = c.df_formula[item](*args)
            except Exception as e:
                value = None
            c.df_category[item].append(value)
        for item in c.html_code_page:
            args = [html]
            args.extend(c.html_code_page[item])
            try:
                value = c.df_formula[item](*args)
            except Exception as e:
                value = None
            c.df_category[item].append(value)

    df = pd.DataFrame.from_dict(c.df_category)
    df.insert(0, 'run_date', run_date)
    df['run_date'] = pd.to_datetime(df['run_date']).dt.date
    df["original_price"] = np.where(df["original_price"] == 0, df["final_price"], df["original_price"])
    return df

def choose_vpn_filename():
    bucket = 'sirclo-data-marketplace'
    prefix = 'assets/ovpn/'
    vpn_profile_list = list_blob_gcs(bucket_name=bucket, prefix_name=prefix)
    vpn_profile_list.remove(prefix)

    picked_profile = random.choice(vpn_profile_list)
    print(f"the profile used is {picked_profile}")

    # download vpn profile from cloud storage
    vpn_filename = picked_profile.split('/')[-1]
    download_blob_to_local(bucket_name=bucket, local_file_name=vpn_filename, gcs_blob_name=picked_profile)
    time.sleep(5)
    return vpn_filename

def main(start: int, end: int, base_path: str, run_date: str, schema_path: str, target_table: str, proxy_use: str = "no", vpn_use="no", sort_by: str = "5"):
    #get_data(page, category, base_dir, bucket_name, base_path, run_date, sort_by)
    opts = ['--headless']
    list_proxy = []
    print(proxy_use)
    if proxy_use == "yes":
        response = requests.get("http://10.103.255.45:8000/indo")
        response_dict = response.json()
        list_proxy = response_dict["address"]
    print(list_proxy)
    if vpn_use == "yes":
        vpn_filename = choose_vpn_filename()
        connect_to_vpn(vpn_filename)
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    categories = get_csv_column("./extract/tokopedia/search_item/tokopedia_categories_full.csv", "Category URL")
    print(run_date)
    for i in range(len(categories)):
        if list_proxy:
            # CHOOSING IP ADDRESS AND SETTING WEBDRIVER
            proxy_ip = random.choice(list_proxy)
            print(proxy_ip)
            opts.append(f'--proxy-server={proxy_ip}')

        driver = set_webdriver(additional_opts=opts, driver_type="webdriver")

        url_category = categories[i]
        print(url_category)
        category = url_category.split('/', 4)[4]
        for page in range(start, end):
            # if message["start_page"] != page or len(categories) > 1:
            #     retry_count = 0
            #     category = category.split('/',4)[4]
            #     message["category_name"] = category
            print(f"Doing page number {page}")
            try:
                driver.get(f"https://tokopedia.com/p/{category}?page={page}&ob={sort_by}")
                time.sleep(5)
                for j in range(20):
                    driver.execute_script("window.scrollBy(0,250)")
                driver.execute_script("window.scrollBy(0,50)")
                html_source = driver.page_source
                html = BeautifulSoup(html_source, 'html.parser')
                print("Page scraped..")
                df = items_to_df(html, run_date)
                df_to_bq(df, target_table, schema_path)
            except Exception as e:
                print(e)
