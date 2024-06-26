import json
import os
import pathlib

import pandas as pd
from bs4 import BeautifulSoup

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import download_blob_to_local, list_blob_gcs
from transform.shopee.shopee_bs.processing import Processing
from transform.shopee.shopee_bs.category_page_dictionary import Dictionary


p = Processing()
d = Dictionary()

def cleansing_html(file_html: str, page: int, base_dir: str, target_table: str, run_date: str):
    read_html = open(f'{base_dir}/{file_html}', 'r')
    soup = BeautifulSoup(read_html, 'html.parser')
    res = soup.find_all('script', type='application/ld+json')

    # get data from application/ld+json
    all_data = p.normalize_json_html(res)
    list_key = [all_data[i] for i in all_data]
    df_1 = pd.concat(list_key, ignore_index=True)

    # drop null if productid null
    try:
        df_1 = df_1.dropna(subset=['productID'])
    except Exception as e:
        print(e)
        print('no need to drop null')

    # print(df_1.shape)
    df_1.info()
    print('df_1 is created')

    # get data from class
    col_add = d.df_item
    res2    = soup.find_all('div', class_="col-xs-2-4 shopee-search-item-result__item")

    for i in res2 : 
        for item in col_add :
            try : 
                # col_add[item].append(i.find('div', class_=d.xpath_class_category_config[item]).get_text())
                if item == 'os1_page' : 
                    col_add[item].append(i.find('span',class_=str(d.xpath_class_category_config[item][0])).get_text())
                elif item == 'os2_page' : 
                    if i.find('div',class_=str(d.xpath_class_category_config[item][0])) is not None : 
                        col_add[item].append('Mall')
                    else : 
                        col_add[item].append('') 
                else : 
                    col_add[item].append(i.find('div',class_=str(d.xpath_class_category_config[item][0])).get_text()) #name jualannya 
            except Exception as e : 
                print(e)
                col_add[item].append('')
                continue

    # print(col_add)
    df_2 = pd.DataFrame(d.df_item)

    print(df_2.info())
    print(df_2.head())
    df_2['os_type'] = df_2.apply(p.get_os_category, axis=1)
    print('df_2 is created')

    df_all = pd.merge(df_1, df_2, how='left', on=['name'])
    df_all.to_csv(f'df_clean_{page}.csv', index=False)

    # add other cleansing in df
    df_all['storeID'] = [val.split('.')[-2] for val in df_all['url']]
    category = file_html.split('.')[0][:-4]
    df_all['category'] = category
    
    # drop unnecessary column
    for col in d.to_drop_col_category :
        try:
            df_all = df_all.drop(columns=[col])
        except Exception as e:
            print(e)
            print(f'column {col} not exist')

    # rename column name
    df_all.columns = df_all.columns.str.replace(r"[.]", "_")
    df_all.columns = df_all.columns.str.replace(r"[@]", "")

    # convert dtype of some columns
    for col in d.list_col_toint_category :
        try:
            df_all[col] = pd.to_numeric(df_all[col])
        except Exception as e:
            print(e)
            df_all[col] = 0
    for col in d.list_col_tofloat_category:
        try:
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce')
        except Exception as e:
            print(e)
            df_all[col] = float(0)

    # finish
    print('df_all is created')
    print(df_all.shape)

    # check data availability
    if 'aggregateRating_type' not in df_all.columns:
        print('scrapping result doesnot fulfill the requirements')
        return

    df_all['run_date'] = run_date
    df_all['run_date'] = pd.to_datetime(df_all['run_date']).dt.date
    
    #add hierarchy_category
    hierarchy_category = p.find_div_class(soup=soup, var='shopee-footer-section__content')
    df_all['hierarchy_category'] = hierarchy_category
    
    df_all.columns = df_all.columns.str.lower()
    #drop duplicate
    df_all = df_all.drop_duplicates(subset=['productid','name'], keep='first').reset_index(drop=True)
    print(df_all.info())

    # test upload table to bq
    json_path = "./transform/shopee/config_dtype_df.json"
    df_to_bq(df=df_all, project_table_bq=target_table, file_path=json_path)
    os.system("rm -rf *.csv")
    return


def main(category_name: str, base_path: str, bucket_name: str, target_table: str, run_date: str):
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    p.download_from_gcs(category=category_name,
                      base_dir=base_dir,
                      base_path=base_path,
                      bucket_name=bucket_name,
                      run_date=run_date)

    filtered_html = [i for i in os.listdir(base_dir) if category_name in i and 'page' in i]
    print(filtered_html)
    num = 0
    for html_file in filtered_html:
        try : 
            cleansing_html(file_html=html_file, page=num, base_dir=base_dir, target_table=target_table, run_date=run_date)
            num += 1
        except Exception as e:
            print(e)
            print("hmtl file is not sufficient")
            continue
