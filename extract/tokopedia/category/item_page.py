import os
import pathlib
import random
import time

from helpers.bigquery_helper import bq_to_df
from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.webdriver_helper import set_webdriver
from pyvirtualdisplay import Display

def get_category(category: str, level=1):
    return category.rsplit('/', 2)[-level]


def get_l3(word: str):
    string = word.replace("-", " ")
    return string.title()


def get_data(page: int, url: str, category: str, base_dir: str, bucket_name: str, base_path: str, run_date: str):
    opts = ['--headless']
    disp = Display(size=(1920, 1080))
    disp.start()
    driver = set_webdriver(driver_type="webdriver")
    try:
        html_filename = None
        driver.get(url)
        print(f"the link {url}")
        try:
            time.sleep(5)
            html = driver.page_source
            html_filename = f"{base_dir}/{category}_item_{str(page)}.html"
            print(html_filename)
            with open(html_filename, "w") as f:
                f.write(html)
            blob_name = f"{base_path}/{run_date}/{category}_item_{str(page)}.html"
            # f"{base_path}/{run_date}/{category}_item_{str(page)}.html"
            upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=html_filename, gcs_blob_name=blob_name)
        except Exception as e:
            print(e)
            pass

        time.sleep(random.uniform(30, 60))
        print(f"successfully write {html_filename}")
    except Exception as e:
        print(e)
        print(f"{url} cannot be opened")
        pass
    driver.quit()
    disp.stop()

def main(category: str, bucket_name: str, base_path: str, run_date: str):
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    category = get_category(category)
    l3_category = get_l3(category)
    query = f"SELECT product_url from sirclo-prod.bronze_marketplace.tokopedia_item_header where run_date = '{run_date}' and l2_category = '{l3_category}' "
    print(query)
    query_result = bq_to_df(query)
    print("Getting url page from table")
    num = 0
    for url in query_result["product_url"]:
        get_data(page=num, url=url, category=category, base_dir=base_dir, bucket_name=bucket_name,
                 base_path=base_path, run_date=run_date)
        print("finish")
        num += 1