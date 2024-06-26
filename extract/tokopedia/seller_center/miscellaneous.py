import glob
import json
import os
import requests

from datetime import datetime, timedelta

from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import set_webdriver
from helpers.cloud_storage_helper import load_gcs_json, load_gcs_string
from helpers.date_helper import (get_on_month_period_date, is_valid_date_range, adjust_end_date)

from pathlib import Path

def get_cookies_from_gcs(official_store_name, sc_page, gcs_logger):
    if sc_page == "statistic":
        identicator_cookies = "cookies"
    elif sc_page == "ads" or sc_page == "cookies_result":
        identicator_cookies = "cookies_result"
    elif sc_page == "operational" or sc_page == "shop_score" or sc_page == "seller_center":
        identicator_cookies = "cookies_operational"
    else:
        raise ValueError(f"The page {sc_page} is not identicated, please check your page")

    gcloud_cookies_path = f"assets/cookies/tokopedia/{official_store_name}/{identicator_cookies}_{official_store_name.replace(' ','_')}.json"

    try:
        if sc_page == "statistic" or sc_page == "ads":
            cookies = load_gcs_json("sirclo-data-marketplace", gcloud_cookies_path, debug=False)
        elif sc_page == "operational" or sc_page == "shop_score" or sc_page == "seller_center":
            cookies = load_gcs_string("sirclo-data-marketplace", gcloud_cookies_path)
        return cookies
    except:
        gcs_logger.log(f"{official_store_name} bucket is not existed or cookies is not created")

def get_cookies(driver, official_store_name, gcs_bucket, cookies_local_path, marketplace_page, gcs_logger):
    # Get cookies
    driver_cookies     = driver.get_cookies()
    cookie_dict        = {}
    cookie_result_dict = {}
    cookie_result      = ""
    tkpd_user_id       = ""

    for cookie in driver_cookies:
        cookie_name   = cookie['name']
        cookie_value  = cookie['value']
        cookie_result = cookie_result + \
            str(cookie_name) + "=" + str(cookie_value) + "; "
        
        cookie_dict[cookie_name] = cookie_value  # For statistic page
        cookie_result_dict['cookie'] = cookie_result  # For ads page

        # Find Tokopedia user id
        if str(cookie['name']) == "tuid":
            tkpd_user_id = str(cookie['value'])
            cookie_result_dict['tkpd_user_id'] = tkpd_user_id

    # Cookies local and gcloud path for each page
    local_cookies_stat_path        = f"{cookies_local_path}/cookies_{official_store_name.replace(' ', '_')}.json"
    local_cookies_ads_path         = f"{cookies_local_path}/cookies_result_{official_store_name.replace(' ', '_')}.json"
    local_cookies_operational_path = f"{cookies_local_path}/cookies_operational_{official_store_name.replace(' ', '_')}.json"

    gcloud_cookies_stat_path        = f"assets/cookies/tokopedia/{official_store_name}/cookies_{official_store_name.replace(' ', '_')}.json"
    gcloud_cookies_ads_past         = f"assets/cookies/tokopedia/{official_store_name}/cookies_result_{official_store_name.replace(' ', '_')}.json"
    gcloud_cookies_operational_path = f"assets/cookies/tokopedia/{official_store_name}/cookies_operational_{official_store_name.replace(' ', '_')}.json"

            
    # Save and upload cookies, for order-operational-shop score page
    if cookie_result is not None:
        with open(local_cookies_operational_path, "w") as cookie_op_file:
            json.dump(cookie_result, cookie_op_file)

        upload_blob_to_gcs(gcs_bucket,
                           local_cookies_operational_path,
                           gcloud_cookies_operational_path)
        sleep_condition(5, 10)

    # Save and upload cookies, for statistic page
    if cookie_dict != {}:
        with open(local_cookies_stat_path, "w") as cookie_file:
            json.dump(cookie_dict, cookie_file)

        upload_blob_to_gcs(gcs_bucket,
                           local_cookies_stat_path, 
                           gcloud_cookies_stat_path)
        sleep_condition(5, 10)

    # Save and upload cookies, for ads page
    if cookie_result_dict != {} and tkpd_user_id != "":
        with open(local_cookies_ads_path, "w") as cookie_result_file:
            json.dump(cookie_result_dict, cookie_result_file)

        upload_blob_to_gcs(gcs_bucket,
                           local_cookies_ads_path, 
                           gcloud_cookies_ads_past)
        sleep_condition(5, 10)

    if marketplace_page == "seller_center" or marketplace_page == "operational" or marketplace_page == "shop_score":
        gcs_logger.log(f"Successfully generate cookies for {marketplace_page}")
        return cookie_result
    elif marketplace_page == "statistic":
        gcs_logger.log(f"Successfully generate cookies for {marketplace_page}")
        return cookie_dict
    elif marketplace_page == "ads":
        gcs_logger.log(f"Successfully generate cookies for {marketplace_page}")
        return cookie_result_dict
    else:
        raise Exception(f"There are no cookies for type : {marketplace_page}")

def send_api_request(url, headers, json_data, shop_id, local_path, gcs_logger):
    condition = False
    response = requests.post(
        url     = url,
        headers = headers,
        json    = json_data)

    status = response.status_code
    sleep_condition(5, 10)
    
    if status == 200:
        try:
            response_json = response.json()

            with open(f"{local_path}/response.json", 'wb') as f:
                f.write(json.dumps(response_json).encode('utf-8'))

            gcs_logger.log(
                f"The data has been stored in {local_path}/response.json")
            condition = True
        except:
            raise Exception(
                f"There is some error for {shop_id}. Please check the error.")
    else:
        gcs_logger.log(
            f"Request encounters error. Status code: {status}, response: {response.content}")

    sleep_condition(5, 10)

    return condition

def remove_file(full_path, type_file=''):
    for path in glob.iglob(os.path.join(full_path, f'*.{type_file}')):
        os.remove(path)

def detect_file_local_dir(full_path, type_file=''):
    folder_path = Path(full_path)
    file_pattern = f"*.{type_file}"

    files = list(folder_path.glob(file_pattern))
    print(files)
    return bool(files)

def driver_init(result_directory=None):
    
    preference = {
        "credentials_enable_service": False,
        "download.prompt_for_download": False,
        "profile.password_manager_enabled": False
    }
    
    if result_directory is not None:
        preference["download.prompt_for_download"] = result_directory

    return set_webdriver(additional_opts=None, exp_opt_pref=preference, driver_type='webdriver')

# Date detection
def date_detection_os(start, end, sc_page, date_identificator_sales=None, date_identificator_ads=None):
    current_date       = datetime.today()
    default_start_date = datetime.strftime(current_date - timedelta(days=6), '%Y-%m-%d')
    default_end_date   = datetime.strftime(current_date, '%Y-%m-%d')

    fix_start_period = start
    fix_end_period   = end

    if start == default_start_date and end == default_end_date:
        if sc_page == "sales":
            start_period = current_date - timedelta(days=date_identificator_sales)
            fix_start_period = datetime.strftime(start_period, '%d/%m/%Y')
            fix_end_period = datetime.strftime(current_date, '%d/%m/%Y')

        elif sc_page == "ads":
            start_period = current_date - timedelta(days=date_identificator_ads)
            end_period = current_date - timedelta(days=1)
            fix_start_period = datetime.strftime(start_period, '%Y-%m-%d')
            fix_end_period = datetime.strftime(end_period, '%Y-%m-%d')

        elif sc_page == "statistic" or sc_page == "operational":
            if datetime.strptime(end, '%Y-%m-%d').day == 1:
                end_period = adjust_end_date(start, end)
                fix_start_period = (datetime.strptime(start, '%Y-%m-%d')).strftime('%d-%m-%Y')
                fix_end_period = (datetime.strptime(end_period, '%Y-%m-%d')).strftime('%d-%m-%Y')
            else:
                start_period, end_period = get_on_month_period_date()
                fix_start_period = (datetime.strptime(start_period, '%Y-%m-%d')).strftime('%d-%m-%Y')
                fix_end_period = (datetime.strptime(end_period, '%Y-%m-%d') - timedelta(days=1)).strftime('%d-%m-%Y')
    else:
        if sc_page == "statistic" or sc_page == "operational":
            if is_valid_date_range(start, end):
                fix_start_period = (datetime.strptime(start, '%Y-%m-%d')).strftime('%d-%m-%Y')
                fix_end_period = (datetime.strptime(end, '%Y-%m-%d')).strftime('%d-%m-%Y')
            else:
                raise ValueError("Date range is invalid.")
            
        elif sc_page == "ads":
            fix_end_period = (datetime.strptime(end, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            
        elif sc_page == "sales":
            fix_start_period = (datetime.strptime(start, '%Y-%m-%d')).strftime('%d/%m/%Y')
            fix_end_period = (datetime.strptime(end, '%Y-%m-%d')).strftime('%d/%m/%Y')

    print(f"Start generate data for {fix_start_period} until {fix_end_period}")

    return fix_start_period, fix_end_period