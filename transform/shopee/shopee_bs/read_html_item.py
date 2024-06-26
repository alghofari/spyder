import json
import sys
import os
import argparse
import pathlib

import pandas as pd
from bs4 import BeautifulSoup, Tag
from google.cloud import storage
from lxml import etree
from datetime import date, datetime

from helpers.cloud_storage_helper import download_blob_to_local, list_blob_gcs
from helpers.bigquery_helper import df_to_bq
from transform.shopee.shopee_bs.category_page_dictionary import Dictionary
from transform.shopee.shopee_bs.processing import Processing


p = Processing()
d = Dictionary()

def cleansing_html(file_html: str, base_dir: str, run_date: str, category: str) : 
    #read html file
    read_html = open(f'{base_dir}/{file_html}', 'r',encoding="utf-8")
    soup      = BeautifulSoup(read_html, 'html.parser')
    dom       = etree.HTML(str(soup))

    data = {}

    #getting data from html
    data['name']    = p.recheck_result_css(soup=soup, var= d.css_item_config['name'])
    if data['name']  == 'n/a' : 
        print("result not valid")
        return

    for item in d.css_item_config : 
        if item != 'name' : 
            data[item] = p.recheck_result_css(soup=soup, var=d.css_item_config[item])


    data['stok']              = p.recheck_result_xpath(dom=dom, var=d.xpath_item_config['stok'])
    data['spesifikasi']       = p.recheck_result_class(var = soup.find_all('div',class_= d.div_findall_item_config['spesifikasi']))

    ts = p.transform_spesifikasi(var=data['spesifikasi'] )
    spesifikasi_cleaned = ts[0]
    data['spesifikasi'] = ' | '.join([str(item) for item in spesifikasi_cleaned])
    data['dikirim_dari'] = ts[1]

    data['nama_os'] = p.recheck_result_css(soup=soup, var=d.css_item_config['nama_os'])
    data['os_type'] = p.get_os_type(dom=dom, var_type_1=d.os_type_xpath['os1_item'], var_type_2=d.os_type_xpath['os2_item'])

    find_toko_penilaian = p.recheck_result_class(var=soup.find_all(class_= d.div_findall_item_config['toko_penilaian']))
    result_tf_penilaian = p.transform_penilaian(var=find_toko_penilaian)
    data['toko_jum_penilaian'] = result_tf_penilaian[0]
    data['toko_persen_dbalas'] = result_tf_penilaian[1]
    data['toko_wkt_dbalas']    = result_tf_penilaian[2]
    data['toko_bergabung']     = result_tf_penilaian[3]
    data['toko_pengikut']      = result_tf_penilaian[4]
    data['toko_jum_produk']    = p.recheck_result_class(soup.find(class_=d.div_find_item_config['toko_jum_produk'])).get_text()

    get_url = p.split_app_json(dom=dom)
    data['url'] = get_url[1]
    data['product_id'] = int(get_url[0])
    data['shop_id'] = int(get_url[2])
    data['additional_info'] = get_url[3]

    data['terjual_detail'] = p.find_class_helper(soup=soup, var=d.div_find_item_config['terjual_detail'])
    # data['category'] = category.split('-cat')[0]
    data['category'] = file_html.split("-cat")[0]
    
    print(data)
    return data

def main(category: str, bucket_name: str, base_path: str, target_table: str, run_date: str, get_prev_date: int, prev_date: str) :
    if int(get_prev_date) == 1 : 
        base_dir = os.getcwd() + f'/{base_path}/{prev_date}'
    else : 
        base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    print(base_dir)
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    print("start downloading")
    #download html file from gcs
    p.download_from_gcs(category=category,
                      base_dir=base_dir, 
                      base_path=base_path,
                      bucket_name=bucket_name,
                      run_date=run_date, 
                      get_prev_date=get_prev_date, 
                      prev_date=prev_date)

    #listing all files needed for further processing
    filtered_html = [i for i in os.listdir(base_dir)]
    print(filtered_html)
    #create empty dataframe
    df = pd.DataFrame(columns=d.col_df_item)
    
    for html_file in filtered_html:
        print(html_file)
        result = cleansing_html(file_html=html_file, base_dir=base_dir, run_date=run_date, category=category) #processing the result
        try : 
            df.loc[len(df)] = result
            print('sufficient input')
        except Exception as e:
            print(e) 
            print("hmtl file is not sufficient")
            continue
    
    run_date         = date.today().strftime("%Y-%m-%d")
    run_datetime     = datetime.strptime(run_date, "%Y-%m-%d")
    if int(get_prev_date) == 1 : 
        df['run_date'] = prev_date
        df['run_date'] = pd.to_datetime(df['run_date'], format="%Y-%m-%d")
    else : 
        df['run_date']   = run_datetime
    df['product_id'] = pd.to_numeric(df['product_id'],errors='coerce')
    df['shop_id']    = pd.to_numeric(df['shop_id'],errors='coerce')
    
    
    json_path = "./transform/shopee/config_dtype_df_item.json"
    df = df.dropna(subset=['name','product_id'])
    df.info()
    df_to_bq(df=df, project_table_bq=target_table, file_path=json_path)