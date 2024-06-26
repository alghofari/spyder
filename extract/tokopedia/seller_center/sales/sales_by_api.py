import os
import pathlib
import requests
import time

from datetime import datetime

from executor.upload_executor import gcs_upload_bucket
from extract.tokopedia.seller_center.miscellaneous import (date_detection_os,
                                                           get_cookies, remove_file, 
                                                           detect_file_local_dir)

def generate_sales_report_id(cookies, shop_id, start_date, end_date, username, time_sleep, gcs_logger):
    gcs_logger.log(f"Start generate report ID for get data seller center from {start_date} - {end_date}")
    
    headers = {
        "authority"          : "gql.tokopedia.com",
        "accept"             : "*/*",
        "accept-language"    : "en-US,en;q=0.9",
        "content-type"       : "application/json",
        "cookie"             : cookies,
        "origin"             : "https://seller.tokopedia.com",
        "referer"            : "https://seller.tokopedia.com/myshop_order?auto=1&status=all_order",
        "sec-ch-ua"          : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile"   : "?0",
        "sec-ch-ua-platform" : '"macOS"',
        "sec-fetch-dest"     : "empty",
        "sec-fetch-mode"     : "cors",
        "sec-fetch-site"     : "same-site",
        "user-agent"         : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "x-source"           : "tokopedia-lite",
        "x-tkpd-lite-service": "icarus"
    }
    json_data = {
        "operationName": "GenerateSalesReport",
        "variables": {
            "shopId"   : int(shop_id),
            "startDate": start_date,
            "endDate"  : end_date,
            "userName" : username
        },
        "query": "mutation GenerateSalesReport($shopId: Int!, $startDate: String!, $endDate: String!, $userName: String!) {generateSalesReport: generate_sales_report(input: {shop_id: $shopId, start_date: $startDate, end_date: $endDate, user_name: $userName}) {reportId: report_id status downloadUrl: download_url reportName: report_name __typename}}"
    }
    
    response = requests.post(
        url     = "https://gql.tokopedia.com/graphql/GenerateSalesReport",
        headers = headers,
        json    = json_data,
        timeout = 300
    )

    time.sleep(time_sleep)

    if response.status_code == 200:
        if 'Maaf, sedang terjadi pemeliharaan sistem. Silakan coba beberapa saat lagi.' in response.text:
            gcs_logger.error(response.text)
            raise ValueError(f"There is some problem in the system")
        else:
            result = response.json()
            print(result)
            report_id = result['data']['generateSalesReport']['reportId']
            return report_id, True
    else:
        gcs_logger.error(response.text)
        return None, False

def download_sales_report(full_path, cookies, report_id, start_date, end_date, time_sleep, gcs_logger):
    gcs_logger.log(f"Start download data for get data seller center")

    start_date = (datetime.strptime(start_date, '%d/%m/%Y')).strftime('%Y%m%d')
    end_date = (datetime.strptime(end_date, '%d/%m/%Y')).strftime('%Y%m%d')
        
    file_name = f"{full_path}/Tokopedia_Order_{start_date}-{end_date}.xlsx"

    headers = {
        'authority'         : 'api.tokopedia.com',
        'accept'            : '*/*',
        'accept-language'   : 'en-US,en;q=0.9',
        'cookie'            : cookies,
        'origin'            : 'https://seller.tokopedia.com',
        'referer'           : 'https://seller.tokopedia.com/myshop_order?status=all_order',
        'sec-ch-ua'         : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile'  : '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest'    : 'empty',
        'sec-fetch-mode'    : 'cors',
        'sec-fetch-site'    : 'same-site',
        'user-agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }
    
    response = requests.get(
        url     = f'https://api.tokopedia.com/order/download_report?report_id={report_id}',
        headers = headers,
        params  = {"report_id": report_id},
        timeout = 300
    )

    time.sleep(time_sleep)

    with open(file_name, 'wb') as f:
        f.write(response.content)
        gcs_logger.log(f"Successfull download data for get data order with interval date {start_date} until {end_date}")

def generate_order_data(webdriver, config, start_date, end_date, gcs_logger):
    
    # get config from spreadsheet
    official_store_name = config[3]
    seller_center_user  = config[4]
    tkpd_shop_id        = config[5]
    time_sleep          = int(config[6])
    date_interval_os    = int(config[7])
    
    gcs_logger.log(f"------------------- ORDER PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate order data for {official_store_name}")

    current_directory = os.getcwd()
    base_path         = "assets/excel/tokopedia/sales"
    excel_path        = f"{current_directory}/{base_path}/{official_store_name}"
    pathlib.Path(excel_path).mkdir(parents=True, exist_ok=True)

    remove_file(full_path=excel_path,
                type_file="xlsx")

    cookie_directory = f"{current_directory}/cookies/tokopedia/{official_store_name}"
    pathlib.Path(cookie_directory).mkdir(parents=True, exist_ok=True)

    remove_file(full_path=cookie_directory,
                type_file="json")
    
    start_period, end_period = date_detection_os(
        start                    = start_date,
        end                      = end_date,
        sc_page                  = "sales",
        date_identificator_sales = date_interval_os
    )
    gcs_logger.log(f"with interval date {start_period} until {end_period}")
    
    cookie_request = get_cookies(
            driver              = webdriver,
            official_store_name = official_store_name,
            gcs_bucket          = "sirclo-data-marketplace",
            cookies_local_path  = cookie_directory,
            marketplace_page    = "seller_center",
            gcs_logger          = gcs_logger
    )

    condition_api = False

    if cookie_request != None:
        sales_report_id, condition_api = generate_sales_report_id(
            cookies    = cookie_request,
            shop_id    = tkpd_shop_id,
            start_date = start_period,
            end_date   = end_period,
            username   = seller_center_user,
            time_sleep = time_sleep,
            gcs_logger = gcs_logger
        )
        gcs_logger.log("Done in generate report")
        
        if condition_api:
            download_sales_report(
                full_path  = excel_path,
                cookies    = cookie_request,
                report_id  = sales_report_id,
                start_date = start_period,
                end_date   = end_period,
                time_sleep = time_sleep,
                gcs_logger = gcs_logger
            )
            gcs_logger.log("Done in download sales report")

    if cookie_request == None and condition_api == False:
        cookie_request = get_cookies(
            driver              = webdriver,
            official_store_name = official_store_name,
            gcs_bucket          = "sirclo-data-marketplace",
            cookies_local_path  = cookie_directory,
            marketplace_page    = "seller_center",
            gcs_logger          = gcs_logger
        )
    
        sales_report_id = generate_sales_report_id(
            cookies    = cookie_request,
            shop_id    = tkpd_shop_id,
            start_date = start_period,
            end_date   = end_period,
            username   = seller_center_user,
            time_sleep = time_sleep,
            gcs_logger = gcs_logger
        )
        gcs_logger.log("Done in generate report")
        
        download_sales_report(
            full_path  = excel_path,
            cookies    = cookie_request,
            report_id  = sales_report_id,
            start_date = start_period,
            end_date   = end_period,
            time_sleep = time_sleep,
            gcs_logger = gcs_logger
        )
        gcs_logger.log("Done in download sales report")

    if detect_file_local_dir(excel_path, type_file="xlsx"):
        gcs_logger.log(f"Excel sales file is detected in directory")

        gcs_upload_bucket(
            os_name     = official_store_name,
            page_name   = "sales",
            marketplace = "tokopedia",
            file_type   = "excel",
            gcs_logger  = gcs_logger
        )