import os
import pathlib

from executor.upload_executor import gcs_upload_bucket
from helpers.time_helper import sleep_condition
from extract.tokopedia.seller_center.miscellaneous import (get_cookies, remove_file, send_api_request,
                                                           get_cookies_from_gcs, detect_file_local_dir)

base_url   = "https://seller.tokopedia.com"
referrer   = "shop-score-page"
api_url    = "https://gql.tokopedia.com/graphql/GetShopScoreLevel"
gcs_bucket = "sirclo-data-marketplace"

def generate_request_data(shop_id, cookies, gcs_logger):
    # Set the header for requests
    headers = {
        "authority"          : "gql.tokopedia.com",
        "accept"             : "*/*",
        "accept-language"    : "en-US,en;q=0.9",
        "content-type"       : "application/json",
        "cookie"             : cookies,
        "origin"             : base_url,
        "referer"            : f'{base_url}/{referrer}',
        "sec-ch-ua"          : '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        "sec-ch-ua-mobile"   : "?0",
        "sec-ch-ua-platform" : "Windows",
        "sec-fetch-dest"     : "empty",
        "sec-fetch-mode"     : "cors",
        "sec-fetch-site"     : "same-site",
        "user-agent"         : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "x-source"           : "tokopedia-lite",
        "x-tkpd-lite-service": "icarus",
        "x-version"          : "8cf1e04"
    }

    # Set the json_data for requests
    json_data = {
        "operationName": "GetShopScoreLevel",
        "variables": {
            "shopIDStr"      : f'{shop_id}',
            "source"         : "icarus",
            "calculateScore" : True,
            "getNextMinValue": True,
            "includeRawData" : True
        },
        "query": "query GetShopScoreLevel($shopIDStr: String!, $source: String!, $calculateScore: Boolean!, $getNextMinValue: Boolean!, $includeRawData: Boolean) { shopScoreLevel(input: {shopID: $shopIDStr, source: $source, calculateScore: $calculateScore, getNextMinValue: $getNextMinValue, includeRawData: $includeRawData}) { result { shopID shopScore shopLevel shopScoreDetail { title identifier value rawValue nextMinValue colorText __typename } period nextUpdate __typename } error { message __typename } __typename } shopLevel(input: {shopID: $shopIDStr, source: $source}) { result { shopID period nextUpdate shopLevel itemSold niv __typename } error { message __typename } __typename } }"
    }

    gcs_logger.log(
        "Successful to generate headers and json data for API request")
    sleep_condition(5, 10)

    return headers, json_data

def generate_shop_score(webdriver, config, gcs_logger):
    # Define the config
    marketplace       = "tokopedia"
    
    # get config from spreadsheet
    official_store_name =config[3]
    tkpd_shop_id = config[5]

    gcs_logger.log(f"------------------- SHOP SCORE PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate order data for {official_store_name}")

    base_dir = os.getcwd()

    result_dir = f"{base_dir}/assets/json/tokopedia/shop_score/{official_store_name}"
    pathlib.Path(result_dir).mkdir(parents=True, exist_ok=True)

    remove_file(
        full_path = result_dir, 
        type_file = "json"
    )

    cookie_dir = f"{base_dir}/cookies/tokopedia/{official_store_name}"
    pathlib.Path(cookie_dir).mkdir(parents=True, exist_ok=True)

    remove_file(full_path=cookie_dir)
    cookie_request = get_cookies_from_gcs(
        official_store_name = official_store_name,
        sc_page             = "shop_score",
        gcs_logger          = gcs_logger
    )
    condition_api = False

    if cookie_request != None:
        headers, json_data = generate_request_data(
            shop_id    = tkpd_shop_id,
            cookies    = cookie_request,
            gcs_logger = gcs_logger
        )

        condition_api = send_api_request(
            url        = api_url,
            headers    = headers,
            json_data  = json_data,
            shop_id    = tkpd_shop_id,
            local_path = result_dir,
            gcs_logger = gcs_logger
        )

    if cookie_request == None and condition_api == False:

        webcookies = get_cookies(
            driver              = webdriver,
            official_store_name = official_store_name,
            gcs_bucket          = "sirclo-data-marketplace",
            cookies_local_path  = cookie_dir,
            marketplace_page    = "shop_score",
            gcs_logger          = gcs_logger
        )

        headers, json_data = generate_request_data(
            shop_id    = tkpd_shop_id,
            cookies    = webcookies,
            gcs_logger = gcs_logger
        )

        send_api_request(
            url        = api_url,
            headers    = headers,
            json_data  = json_data,
            shop_id    = tkpd_shop_id,
            local_path = result_dir,
            gcs_logger = gcs_logger
        )

        gcs_logger.log("Finish all pipeline with new cookies")

    if condition_api == False:
        raise Exception(f"There might be an error when generating the shop_score page from OS {official_store_name}")

    # display.stop()
    if detect_file_local_dir(result_dir, type_file="json"):

        gcs_upload_bucket(
            os_name     = official_store_name,
            page_name   = "shop_score",
            marketplace = marketplace,
            file_type   = "json",
            gcs_logger  = gcs_logger
        )
        
    else:
        gcs_logger.log(f"Json shop_score file is not detected in directory")
