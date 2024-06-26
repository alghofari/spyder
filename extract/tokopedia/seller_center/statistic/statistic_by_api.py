# --- Import all the packages ---
import base64
import requests
import os
import json
import pathlib

from executor.upload_executor import gcs_upload_bucket
from helpers.time_helper import sleep_condition
from extract.tokopedia.seller_center.miscellaneous import (get_cookies, remove_file, get_cookies_from_gcs,
                                                           detect_file_local_dir, date_detection_os)

def send_request_api(cookie, tkpd_shop_id, json_page, header_page, start_date, end_date, localpath, gcs_logger):
    # Set the header for requests
    header_request = {
        "authority"         : "api.tokopedia.com",
        "accept"            : "*/*",
        "accept_language"   : "en-US,en;q=0.9",
        "content-type"      : "application/json",
        "origin"            : "https://seller.tokopedia.com",
        "referer"           : f"https://seller.tokopedia.com/statistic/{header_page}",
        "sec-ch-ua-mobile"  : "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest"    : "empty",
        "sec-fetch-mode"    : "cors",
        "sec-fetch-site"    : "same-site",
        "user-agent"        : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }

    # Set the json_data for requests
    json_request = {
        "shopId"   : int(tkpd_shop_id),
        "startDate": start_date,
        "endDate"  : end_date,
        "dateType" : "custom",
        "pageName" : f"{json_page}"
    }

    sleep_condition(5,10)

    condition = ""
    response  = requests.post(url    = "https://api.tokopedia.com/dashboard-rest/statistic/download-data",
                             cookies = cookie,
                             headers = header_request,
                             json    = json_request)

    status = response.status_code
    sleep_condition(5,10)
    
    if status == 200:
        try:
            response_json = response.json()
            encoded_file  = response_json['data']['EncodedFile']
            decoded_file  = base64.b64decode(encoded_file)

            file_name = response_json['data']['FileName']

            with open(f"{localpath}/{file_name}", 'wb') as f:
                f.write(decoded_file)

            condition = True

        except Exception as error_code:
            condition = False
            gcs_logger.log(f"There is some error for {tkpd_shop_id}. Please check the error:\n {error_code}")
            
    else:
        condition = False
        gcs_logger.log(f"Get an error based on status {status}\nwith content: {response.content}")
    
    return condition

# --- Set the main pipeline ---
def generate_statistic_data(webdriver, config, page_part, start_date, end_date, gcs_logger):
    # Define the config
    official_store_name =config[3]
    tkpd_shop_id = config[5]
    
    gcs_logger.log(f"------------------- STATISTICS PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate statistics data for {official_store_name}")

    current_directory    = os.getcwd()
    json_identifier_path = f"{current_directory}/extract/tokopedia/seller_center/statistic/page.json"

    with open(json_identifier_path, "r") as file:
        json_identifier = json.load(file)

    if page_part != "all":
        identificator = [page_part]
    else:
        identificator = list(json_identifier['statistic'].keys())

    for page in identificator:
        identification = json_identifier['statistic'][page]
        name_homepage  = identification['homepage']
        req_head       = identification['header']
        req_pay        = identification['payload']

        assets_file = name_homepage.lower().replace(" ", "_")
        base_path   = f"assets/excel/tokopedia/{assets_file}/{official_store_name}"

        result_directory = f"{current_directory}/{base_path}"
        pathlib.Path(result_directory).mkdir(parents=True, exist_ok=True)

        remove_file(
            full_path=result_directory,
            type_file="xlsx"
        )

        cookie_directory = f"{current_directory}/cookies/tokopedia/{official_store_name}"
        pathlib.Path(cookie_directory).mkdir(parents=True, exist_ok=True)

        remove_file(
            full_path=cookie_directory,
            type_file="json"
        )

        start_period, end_period = date_detection_os(
            start   = start_date,
            end     = end_date,
            sc_page = "statistic"
        )
        gcs_logger.log(f"with interval date {start_period} until {end_period}")
        
        cookies_request = get_cookies_from_gcs(
        official_store_name = official_store_name,
        sc_page             = "statistic",
        gcs_logger          = gcs_logger
        )
        condition_api = False

        if cookies_request != None:
            # Case 1: Where the cookies is already on GCS Bucket and ready to use
            condition_api = send_request_api(
                cookie       = cookies_request,
                tkpd_shop_id = tkpd_shop_id,
                json_page    = req_pay,
                header_page  = req_head,
                start_date   = start_period,
                end_date     = end_period,
                localpath    = result_directory,
                gcs_logger   = gcs_logger
            )

        if condition_api == False or cookies_request == None:
            # Case 2: Where the cookies from gcs is expired or cookies is not detected in GCS Bucket
            
            webcookies = get_cookies(
                driver              = webdriver,
                official_store_name = official_store_name,
                gcs_bucket          = "sirclo-data-marketplace",
                cookies_local_path  = cookie_directory,
                marketplace_page    = "statistic",
                gcs_logger          = gcs_logger
            )
            
            condition_api = send_request_api(
                cookie       = webcookies,
                tkpd_shop_id = tkpd_shop_id,
                json_page    = req_pay,
                header_page  = req_head,
                start_date   = start_period,
                end_date     = end_period,
                localpath    = result_directory,
                gcs_logger   = gcs_logger
            )

            gcs_logger.log(f"Finish all pipeline for {assets_file} with new cookies")

            if condition_api == False:
                raise Exception(f"There might be an error when generating the {req_head} page from OS {official_store_name}")

        if detect_file_local_dir(result_directory, type_file="xlsx"):

            gcs_upload_bucket(
                os_name     = official_store_name,
                page_name   = assets_file,
                marketplace = "tokopedia",
                file_type   = "excel",
                gcs_logger  = gcs_logger
            )
        
        else:
            gcs_logger.log(f"Excel {assets_file} file is not detected in directory")