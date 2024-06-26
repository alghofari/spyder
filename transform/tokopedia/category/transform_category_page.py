# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 14:55:01 2022

@author: SIRCLO
"""

import os
import pathlib

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import time
from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import download_blob_to_local, list_blob_gcs
from transform.tokopedia.category_page_dictionary import Category


def get_category(category: str, level=1):
    return category.rsplit('/', 2)[-level]


def download_from_gcs(category: str, base_dir: str, base_path: str, bucket_name: str, run_date: str):
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/{run_date}/{category}")
    for index, file in enumerate(list_blob):
        print(f"list file in blob gcs: {file}")
        file_name = f'{base_dir}/{category}_page_{str(index)}.html'
        download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
        print(f'download finish for {category}{index}')


def cleansing_html(file_html: str, base_dir: str, target_table: str, run_date: str):
    c = Category()
    read_html = open(f'{base_dir}/{file_html}', 'r', encoding="utf-8")
    html = BeautifulSoup(read_html, 'html.parser')
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
    df.info()
    print('df is created')
    print(df.shape)
    path = "./transform/tokopedia/config_dtype_df.json"
    df_to_bq(df, target_table, path)

    return


def main(category: str, base_path: str, bucket_name: str, target_table: str, run_date: str):
    category = get_category(category)
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    download_from_gcs(category=category,
                      base_dir=base_dir,
                      base_path=base_path,
                      bucket_name=bucket_name,
                      run_date=run_date)
    filtered_html = [i for i in os.listdir(base_dir) if category in i]

    print(os.listdir(base_dir))
    for html_file in filtered_html:
        print("doing html")
        try:
            cleansing_html(file_html=html_file, base_dir=base_dir, target_table=target_table, run_date=run_date)
        except Exception as e:
            print(e)
            continue