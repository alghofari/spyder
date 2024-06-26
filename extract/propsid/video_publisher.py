import json
import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.common.by import By

from helpers.dataframe_helper import datatype_casting


def extract(driver, start_date, end_date, schema_json):
    url = f'https://dash.props.id/ads/ajax/report/publisher/?partner=13&domain=&date={start_date}+-+{end_date}'
    driver.get(url)
    resp_body = driver.find_element(By.TAG_NAME, "body").text
    data = json.loads(resp_body)['data']
    dataframe = pd.DataFrame(data, columns=['date', 'revenue', 'impression', 'cpm', 'viewability', 'ctr'])
    dataframe['load_timestamp'] = datetime.now()
    datatype_casting(schema_json, dataframe)
    return dataframe


def load(dataframe, table_id, auto_detect, table_schema, write_disposition, create_disposition):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=auto_detect,
        schema=table_schema,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
    )
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()
    table = client.get_table(table_id)
    print("Loaded {} rows to {}.".format(table.num_rows, table_id))


def main(driver: webdriver):
    start_date = datetime.strftime(datetime.now() - timedelta(days=60), '%Y-%m-%d')
    end_date = datetime.strftime(datetime.now(), '%Y-%m-%d')

    base_path = os.getcwd() + "/extract/propsid"

    schema_path = base_path + "/schema/video_publisher.json"
    schema_file = open(schema_path)
    schema_json = json.load(schema_file)

    try:
        df = extract(driver, start_date, end_date, schema_json)
        load(
            dataframe=df,
            table_id="sirclo-prod.bronze_ads_orami.props_video_publisher_report",
            auto_detect=False,
            table_schema=schema_json,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )
    except Exception as e:
        print(e)
        pass
