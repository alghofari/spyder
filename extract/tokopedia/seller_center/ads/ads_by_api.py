# --- Import all the packages ---
import requests
import os
import pathlib
import time

from executor.upload_executor import gcs_upload_bucket
from extract.tokopedia.seller_center.miscellaneous import (date_detection_os, get_cookies, 
                                                           get_cookies_from_gcs, remove_file, 
                                                           detect_file_local_dir)

    # --- Function to send request to get data in specific range date ---
def send_request_ads_data(tkpd_shop_id, cookies_string, start_date, end_date, time_sleep, gcs_logger):

    # Set the header for requests
    header_request = {
        "authority"          : "gql.tokopedia.com",
        "path"               : "/graphql/TopadsSubmitStatisticsReportRequest",
        "accept"             : "*/*",
        "accept_language"    : "en-US,en;q=0.9",
        "content-type"       : "application/json",
        "cookie"             : cookies_string,
        "origin"             : "https://ta.tokopedia.com",
        "referer"            : f"https://ta.tokopedia.com/v2/manage",
        "sec-ch-ua"          : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile"   : "?0",
        "sec-ch-ua-platform" : '"Windows"',
        "sec-fetch-dest"     : "empty",
        "sec-fetch-mode"     : "cors",
        "sec-fetch-site"     : "same-site",
        "user-agent"         : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "x-source"           : "tokopedia-lite",
        "x-tkpd-lite-service": "midas"
    }

    # Set the json_data for requests
    json_request = {
        "operationName": "TopadsSubmitStatisticsReportRequest",
        "query"        : "mutation TopadsSubmitStatisticsReportRequest($queryInput: SubmitStatisticsReportInputType!) {TopadsSubmitStatisticsReportRequest(queryInput: $queryInput) {data {status {error_code message __typename } __typename} errors {code detail title __typename} __typename}}",
        "variables"    : {
            "queryInput" : {
                "shop_id"     : f"{tkpd_shop_id}",
                "start_date"  : f"{start_date}",
                "end_date"    : f"{end_date}",
                "request_type": "1,3"
            }
        }
    }

    # Set the condition API
    condition_api = ""
    response = requests.post(url     = "https://gql.tokopedia.com/graphql/TopadsSubmitStatisticsReportRequest",
                             headers = header_request,
                             json    = json_request)
    
    response_json = response.json()
    error_status  = response_json['data']['TopadsSubmitStatisticsReportRequest']['data']['status']['error_code']
    error_message = response_json['data']['TopadsSubmitStatisticsReportRequest']['data']['status']['message']

    if error_status != 0 and error_message != "OK":
        gcs_logger.log(f"Here is the error status: {error_status}\nand the message:{error_message}")

        condition_api = False
        raise Exception("Send request failed, can't generate ads data")
    
    else:
        condition_api = True
        gcs_logger.log("Request Ads Data has been sent to API")

    time.sleep(time_sleep)
    return condition_api

# --- Function to get specific report ID for Ads Data---
def generate_report_id_ads(cookies_string, tkpd_shop_id, time_sleep, gcs_logger):
    
    # Set the header for get report id
    header_request = {
        "authority"          : "gql.tokopedia.com",
        "accept"             : "*/*",
        "accept_language"    : "en-US,en;q=0.9",
        "content-type"       : "application/json",
        "cookie"             : cookies_string,
        "origin"             : "https://ta.tokopedia.com",
        "referer"            : "https://ta.tokopedia.com/v2/manage",
        "sec-ch-ua"          : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile"   : "?0",
        "sec-ch-ua-platform" : '"Windows"',
        "sec-fetch-dest"     : "empty",
        "sec-fetch-mode"     : "cors",
        "sec-fetch-site"     : "same-origin",
        "user-agent"         : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "x-source"           : "tokopedia-lite",
        "x-tkpd-lite-service": "midas"
    }

    # Set the json_data for get report id
    json_request = {
        "operationName": "TopadsStatisticsGetReportListRequest",
        "query"        : "query TopadsStatisticsGetReportListRequest($shopID: String!) {TopadsStatisticsGetReportListRequest(shop_id: $shopID) {data {data {id name start_date end_date request_type status __typename} __typename} errors {code detail title __typename} __typename}}",
        "variables"    : {
            "shopID"   : f"{tkpd_shop_id}"
        }
    }

    # Send the request for get the report id
    response = requests.post(url     = "https://gql.tokopedia.com/graphql/TopadsStatisticsGetReportListRequest",
                             headers = header_request,
                             json    = json_request)

    response_json   = response.json()
    report_id       = response_json['data']['TopadsStatisticsGetReportListRequest']['data']['data'][0]['id']
    report_filename = response_json['data']['TopadsStatisticsGetReportListRequest']['data']['data'][0]['name']

    time.sleep(time_sleep)

    if report_id != None and report_filename != None:
        gcs_logger.log(f"Successfully generate report id : {report_id}, with file {report_filename}")
        
        return report_id, report_filename
    
    else:
        raise Exception("Report Id is not existing, please try again later")
    
# --- Function to get the content of the data ---
def generate_excel_ads_data(cookies_string, tkpd_user_id, tkpd_shop_id, report_id, local_filepath, report_filename, time_sleep, gcs_logger):

    # Set filename
    filename = f"{local_filepath}/{report_filename}.xlsx"

    # Set the header for download data ads
    header_request = {
        "authority"         : "ta.tokopedia.com",
        "accept"            : "*/*",
        "accept_language"   : "en-US,en;q=0.9",
        "cookie"            : cookies_string,
        "referer"           : "https://ta.tokopedia.com/v2/manage",
        "sec-ch-ua"         : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile"  : "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest"    : "empty",
        "sec-fetch-mode"    : "cors",
        "sec-fetch-site"    : "same-site",
        "user-agent"        : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "x-tkpd-userid"     : f"{tkpd_user_id}"
    }

    # Send the request for download data ads
    response = requests.get(
        url     = f"https://ta.tokopedia.com/v2/statistic/report/download?shop_id={tkpd_shop_id}&request_id={report_id}",
        headers = header_request)
    
    time.sleep(time_sleep)
    
    if response.content != None:
        with open(filename, 'wb') as f:
            f.write(response.content)
        
        gcs_logger.log(f"Successfully generate the report Ads Data into xlsx file in {report_filename}.xlsx")
    
    else:
        raise Exception("Data Ads can't be generated, please try again later")

# --- Set the main pipeline ---
def generate_ads_data(webdriver, config, start_date, end_date, gcs_logger):
    # Define the config
    marketplace       = "Tokopedia"
    
    # get config from spreadsheet
    official_store_name = config[3]
    tkpd_shop_id        = config[5]
    time_sleep          = int(config[6])
    date_interval_os    = int(config[8])

    gcs_logger.log(f"------------------- ADS PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate ads data for {official_store_name}")

    current_directory = os.getcwd()
    base_path = f"assets/excel/{marketplace.lower()}/ads/{official_store_name}"

    result_directory = f"{current_directory}/{base_path}"
    pathlib.Path(result_directory).mkdir(parents=True, exist_ok=True)

    # Clean directory
    remove_file(full_path = result_directory,
                type_file = "xlsx"
    )

    cookie_directory = f"{current_directory}/cookies/{marketplace.lower()}/{official_store_name}"
    pathlib.Path(cookie_directory).mkdir(parents=True, exist_ok=True)

    # Clean directory
    remove_file(full_path = result_directory,
                type_file = "json"
    )

    start_period, end_period = date_detection_os(
        start                  = start_date,
        end                    = end_date,
        sc_page                = "ads",
        date_identificator_ads = date_interval_os
    )
    gcs_logger.log(f"with interval date {start_period} until {end_period}")

    cookie_request = get_cookies_from_gcs(
        official_store_name = official_store_name,
        sc_page             = "ads",
        gcs_logger          = gcs_logger
    )
    condition_api = False

    if cookie_request != None:
        # Generate the pipeline
        condition_api = send_request_ads_data(
            tkpd_shop_id   = tkpd_shop_id,
            cookies_string = cookie_request['cookie'],
            start_date     = start_period,
            end_date       = end_period,
            time_sleep     = time_sleep,
            gcs_logger     = gcs_logger
        )

        if condition_api:
            report_id, report_filename = generate_report_id_ads(
                cookies_string = cookie_request['cookie'],
                tkpd_shop_id   = tkpd_shop_id,
                time_sleep     = time_sleep,
                gcs_logger     = gcs_logger)
            
            generate_excel_ads_data(
                cookies_string  = cookie_request['cookie'],
                tkpd_user_id    = cookie_request['tkpd_user_id'],
                tkpd_shop_id    = tkpd_shop_id,
                report_id       = report_id,
                local_filepath  = result_directory,
                report_filename = report_filename,
                time_sleep      = time_sleep,
                gcs_logger      = gcs_logger)
            
            gcs_logger.log("Finish all pipeline for ads with old cookies")

    elif cookie_request == None and condition_api == False:
        
        webcookies = get_cookies(
            driver              = webdriver,
            official_store_name = official_store_name,
            gcs_bucket          = "sirclo-data-marketplace",
            cookies_local_path  = cookie_directory,
            marketplace_page    = "ads",
            gcs_logger          = gcs_logger
        )

        condition_api = send_request_ads_data(
            tkpd_shop_id   = tkpd_shop_id,
            cookies_string = webcookies['cookie'],
            start_date     = start_period,
            end_date       = end_period,
            time_sleep     = time_sleep,
            gcs_logger     = gcs_logger
        )

        report_id, report_filename = generate_report_id_ads(
            cookies_string = webcookies['cookie'],
            tkpd_shop_id   = tkpd_shop_id,
            time_sleep     = time_sleep,
            gcs_logger     = gcs_logger
        )

        generate_excel_ads_data(
            cookies_string  = webcookies['cookie'],
            tkpd_user_id    = webcookies['tkpd_user_id'],
            tkpd_shop_id    = tkpd_shop_id,
            report_id       = report_id,
            local_filepath  = result_directory,
            report_filename = report_filename,
            time_sleep      = time_sleep,
            gcs_logger      = gcs_logger
        )

        gcs_logger.log("Finish all pipeline for ads with new cookies")
        
        if condition_api == False:
            raise Exception(f"There might be an error when generating the statistic page from OS {official_store_name}")

    if detect_file_local_dir(result_directory, type_file="xlsx"):

        gcs_upload_bucket(
            os_name     = official_store_name,
            page_name   = "ads",
            marketplace = marketplace.lower(),
            file_type   = "excel",
            gcs_logger  = gcs_logger
        )
    
    else:
        gcs_logger.log(f"Excel ads file is not detected in directory")