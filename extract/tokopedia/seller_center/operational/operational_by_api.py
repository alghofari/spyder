import os
import pathlib

from executor.upload_executor import gcs_upload_bucket
from helpers.time_helper import sleep_condition
from extract.tokopedia.seller_center.miscellaneous import (date_detection_os, get_cookies, remove_file, 
                                                           send_api_request, get_cookies_from_gcs,
                                                           detect_file_local_dir)

base_url   = "https://seller.tokopedia.com"
referrer   = "statistic/operational"
api_url    = "https://gql.tokopedia.com/graphql/GetSearchTable"
gcs_bucket = "sirclo-data-marketplace"

def generate_request_data(shop_id, cookies, start_date, end_date, gcs_logger):
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
    json_data = [{
        "operationName": "GetSearchTable",
        "variables": {
            "dataKeys": [{
                "key"        : "communicationRate",
                "parameters" : f"""{{
                    "start_date" : "{start_date}",
                    "end_date"   : "{end_date}",
                    "page_source": "operational-insight",
                    "date_type"  : "day",
                    "shop_id"    : "{shop_id}",
                    "location_id": 0,
                    "keyword"    : "",
                    "limit"      : 2,
                    "page"       : 1,
                    "sort_index" : 0,
                    "sort_type"  : "descending"
                }}"""
            }
            ]
        },
        "query": "fragment ActionCardFrag on actionCard { summary title subtitle buttonText ctaLink image footer __typename } fragment DropdownCardFrag on dropdownCard { summary title subtitle buttonText ctaLink image footer metaData __typename } fragment MenuContentFrag on dropdownMenuContent { ... on actionCard { ...ActionCardFrag __typename } ... on dropdownCard { ...DropdownCardFrag __typename } __typename } query GetSearchTable($dataKeys: [dataKey!]!) { fetchSearchTableWidgetData(dataKeys: $dataKeys) { data { dataKey data { headers { title tooltip width sortAble sortActive align iconTooltip __typename } rows { columns { value additionalValue type meta data { ... on dropdownData { buttonContent buttonBehaviorType menuContent { ...MenuContentFrag __typename } __typename } __typename } __typename } id __typename } pagination { hasNext hasPrev currentPage totalPage __typename } __typename } error errorMsg showWidget updateInfo showPaywall __typename } __typename } }"
    },
        {
        "operationName": "GetSearchTable",
        "variables": {
            "dataKeys": [{
                "key"        : "communicationSpeed",
                "parameters" : f"""{{
                    "start_date" : "{start_date}",
                    "end_date"   : "{end_date}",
                    "page_source": "operational-insight",
                    "date_type"  : "day",
                    "shop_id"    : "{shop_id}",
                    "location_id": 0,
                    "keyword"    : "",
                    "limit"      : 2,
                    "page"       : 1,
                    "sort_index" : 0,
                    "sort_type"  : "descending"
                }}"""
            }
            ]
        },
        "query": "fragment ActionCardFrag on actionCard { summary title subtitle buttonText ctaLink image footer __typename } fragment DropdownCardFrag on dropdownCard { summary title subtitle buttonText ctaLink image footer metaData __typename } fragment MenuContentFrag on dropdownMenuContent { ... on actionCard { ...ActionCardFrag __typename } ... on dropdownCard { ...DropdownCardFrag __typename } __typename } query GetSearchTable($dataKeys: [dataKey!]!) { fetchSearchTableWidgetData(dataKeys: $dataKeys) { data { dataKey data { headers { title tooltip width sortAble sortActive align iconTooltip __typename } rows { columns { value additionalValue type meta data { ... on dropdownData { buttonContent buttonBehaviorType menuContent { ...MenuContentFrag __typename } __typename } __typename } __typename } id __typename } pagination { hasNext hasPrev currentPage totalPage __typename } __typename } error errorMsg showWidget updateInfo showPaywall __typename } __typename } } "
    }]

    gcs_logger.log(
        "Successful to generate headers and json data for API request")
    sleep_condition(5, 10)

    return headers, json_data

def generate_operational_data(webdriver, config, start_date, end_date, gcs_logger):
    # get config from spreadsheet
    official_store_name =config[3]
    tkpd_shop_id = config[5]

    gcs_logger.log(f"------------------- OPERATIONAL PAGE -------------------")
    gcs_logger.log(f"Start Pipeline for generate operational data for {official_store_name}")

    base_dir   = os.getcwd()

    result_dir = f"{base_dir}/assets/json/tokopedia/operational/{official_store_name}"
    pathlib.Path(result_dir).mkdir(parents=True, exist_ok=True)
    gcs_logger.log(f"Result directory: {result_dir}")

    remove_file(
        full_path = result_dir, 
        type_file = "json"
    )

    cookie_dir = f"{base_dir}/cookies/tokopedia/{official_store_name}"
    pathlib.Path(cookie_dir).mkdir(parents=True, exist_ok=True)

    remove_file(
        full_path = cookie_dir,
        type_file = "json"
    )

    start_period, end_period = date_detection_os(
        start   = start_date,
        end     = end_date,
        sc_page = "operational"
    )
    gcs_logger.log(f"with interval date {start_period} until {end_period}")

    cookie_request = get_cookies_from_gcs(
        official_store_name = official_store_name,
        sc_page             = "operational",
        gcs_logger          = gcs_logger
    )
    condition_api = False

    if cookie_request != None:
        headers, json_data = generate_request_data(
            shop_id    = tkpd_shop_id,
            cookies    = cookie_request,
            start_date = start_period,
            end_date   = end_period,
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
            gcs_bucket          = gcs_bucket,
            cookies_local_path  = cookie_dir,
            marketplace_page    = "operational",
            gcs_logger          = gcs_logger
        )

        headers, json_data = generate_request_data(
            shop_id    = tkpd_shop_id,
            cookies    = webcookies,
            start_date = start_period,
            end_date   = end_period,
            gcs_logger = gcs_logger
        )

        send_api_request(
            url        = api_url,
            headers    = headers,
            json_data  = json_data,
            shop_id    = tkpd_shop_id,
            local_path = result_dir,
            gcs_logger = gcs_logger)
        
        gcs_logger.log("Finish all pipeline with new cookies")

    if condition_api == False:
        raise Exception(f"There might be an error when generating the operational page from OS {official_store_name}")
    
    if detect_file_local_dir(result_dir, type_file="json"):

        gcs_upload_bucket(
            os_name     = official_store_name,
            page_name   = "operational",
            marketplace = "tokopedia",
            file_type   = "json",
            gcs_logger  = gcs_logger
        )
    
    else:
        gcs_logger.log(f"Json operational file is not detected in directory")
