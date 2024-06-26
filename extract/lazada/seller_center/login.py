import os
import time

import gspread
from selenium.webdriver.common.by import By


def read_config():
    gc = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sh = gc.open_by_key('1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg')
    worksheet = sh.worksheet("lazada")
    return worksheet.get_all_records()


def login(driver, username, password):
    # go to login page
    driver.get("https://sellercenter.lazada.co.id")
    time.sleep(5)

    # input email address
    driver.find_element(by=By.XPATH, value='//input[@id="account"]').send_keys(username)
    time.sleep(5)

    # input password
    driver.find_element(by=By.XPATH, value='//input[@id="password"]').send_keys(password)
    time.sleep(5)

    # click login button
    driver.find_element(by=By.XPATH, value='//button[@type="submit"]').click()
    time.sleep(60)


def main(driver, store_name):
    configs = read_config()
    filtered_config = [d for d in configs if d['os_name'] == store_name]

    for config in filtered_config:
        username = config['os_email']
        password = config['password']

        try:
            login(driver, username, password)
        except Exception as e:
            print(f"can not login from {store_name}")
            print(e)
            driver.quit()
            continue
