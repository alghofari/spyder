import os
import pathlib
import time

import requests

from executor.upload_executor import gcs_upload_bucket
from extract.tokopedia.seller_center.miscellaneous import detect_file_local_dir


def export_data(cookies, file_path, start_date, end_date):
    headers = {
        'authority': 'ccp.tokopedia.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': cookies,
        'origin': 'https://www.tokopedia.com',
        'referer': 'https://www.tokopedia.com/play/live',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    params = {
        'start_date': start_date,
        'end_date': end_date,
    }

    response = requests.get('https://ccp.tokopedia.com/v1/report/excel', params=params, headers=headers)

    file_name = file_path + f"/Tokopedia_Live_{start_date}_{end_date}.xlsx"
    with open(file_name, 'wb') as f:
        f.write(response.content)


def main(webdriver, config, start_date, end_date, gcs_logger):
    # get config from spreadsheet
    official_store_name = config[3]

    gcs_logger.log(f"------------------- VIDEO STATISTIC PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate video statistic data for {official_store_name}")

    current_directory = os.getcwd()
    base_path = "assets/excel/tokopedia/wawasan_video"
    excel_path = f"{current_directory}/{base_path}/{official_store_name}"
    pathlib.Path(excel_path).mkdir(parents=True, exist_ok=True)

    gcs_logger.log(f"with interval date {start_date} until {end_date}")

    # open video statistic page
    live_url = "https://www.tokopedia.com/play/live"
    webdriver.get(live_url)
    gcs_logger.log(f"Open video statistic page {live_url}")
    time.sleep(5)

    # get cookies
    webcookies = webdriver.get_cookies()
    webcookies_str = ""
    for cookies in webcookies:
        cookies_name = cookies['name']
        cookies_value = cookies['value']
        webcookies_str = webcookies_str + str(cookies_name) + "=" + str(cookies_value) + "; "
    gcs_logger.log("Get cookies video statistic")
    time.sleep(5)

    # export video statistic report
    export_data(webcookies_str, excel_path, start_date, end_date)
    gcs_logger.log("Done in download video statistic report")

    # upload video statistic report file to gcs bucket
    if detect_file_local_dir(excel_path, type_file="xlsx"):
        gcs_logger.log(f"Excel video statistic file is detected in directory")

        gcs_upload_bucket(
            os_name=official_store_name,
            page_name="wawasan_video",
            marketplace="tokopedia",
            file_type="excel",
            gcs_logger=gcs_logger
        )
