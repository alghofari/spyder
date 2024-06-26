import os
import time
from datetime import datetime

import gspread
from selenium.webdriver.common.by import By


def read_config():
    gc = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sh = gc.open_by_key('1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg')
    worksheet = sh.worksheet("tiktok")
    return worksheet.get_all_records()


def skip_pop_up(driver, css_value):
    try:
        driver.find_element(by=By.CSS_SELECTOR, value=css_value).click()
        time.sleep(1)
    except Exception as e:
        print(e)
        pass


def download_order(driver, start_time_epoch, end_time_epoch, wait_time):
    # go to order page
    url = f"https://seller-id.tiktok.com/order"
    driver.get(url)
    time.sleep(15)

    # click update delivery status tips if exist
    skip_pop_up(driver, css_value="button.theme-arco-btn-primary:nth-child(1) > span:nth-child(1)")

    # click new export order function tips if exist
    skip_pop_up(driver, css_value="button.theme-arco-btn-primary:nth-child(1) > span:nth-child(1)")

    # click data compass navigation menu is upgraded tips if exist
    skip_pop_up(driver, css_value=".arco-btn-secondary > span:nth-child(1)")

    # click pesanan digabungkan tips if exist
    skip_pop_up(driver, css_value="button.theme-arco-btn-primary:nth-child(1) > span:nth-child(1)")

    # go to all order page
    url = f"https://seller-id.tiktok.com/order?selected_sort=6&tab=all&time_order_created[]={start_time_epoch}&time_order_created[]={end_time_epoch}"
    driver.get(url)
    time.sleep(15)

    # click export orders button
    export_orders_button = driver.find_element(by=By.CSS_SELECTOR, value="#order_export_bar > div:nth-child(1) > button:nth-child(1) > span:nth-child(1)")
    export_orders_button.click()
    time.sleep(5)

    # click export button
    export_button = driver.find_element(by=By.CSS_SELECTOR, value="button.theme-arco-btn-primary:nth-child(1) > span:nth-child(1)")
    export_button.click()
    time.sleep(wait_time)

    # click download button
    download_button = driver.find_element(by=By.CSS_SELECTOR, value="div.p-12:nth-child(2) > button:nth-child(2) > div:nth-child(2)")
    download_button.click()
    time.sleep(5)


def main(driver, start_date, end_date, store_name):
    configs = read_config()
    filtered_config = [d for d in configs if d['store_name'] == store_name]

    for config in filtered_config:
        wait_time = config['wait_time']

        format_number = lambda n: n if n % 1 else int(n)
        start_time = start_date + " 00:00:00"
        end_time = end_date + " 23:59:59"
        start_time_epoch = format_number(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp())
        end_time_epoch = format_number(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp())

        try:
            download_order(driver, start_time_epoch, end_time_epoch, wait_time)
        except Exception as e:
            print(f"not able to collect order data from {store_name}")
            print(e)
            continue
