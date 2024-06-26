import json
import gcsfs

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as expect

from helpers.time_helper import sleep_condition

def click_action(driver, wait_time = 300, css_selector = None, xpath = None):
    if css_selector:
        return WebDriverWait(driver, wait_time, 1).until(
            expect.visibility_of_element_located(
            (By.CSS_SELECTOR, css_selector)))
    else:
        return WebDriverWait(driver, wait_time, 1).until(
            expect.visibility_of_element_located(
            (By.XPATH, xpath)))
    

def get_json(official_store_name):
    fs = gcsfs.GCSFileSystem(project="sirclo-data-prod")

    with fs.open(f"gs://sirclo-data-marketplace/assets/excel/shopee/sales/{official_store_name}/credentials/cookies.json", 'r') as f:
        json_object = json.load(f)

    return json_object

def input_password(driver, seller_center_pass):
    try:
        click_action(driver, css_selector=".shopee-input__inner.shopee-input__inner--large .shopee-input__input").send_keys(seller_center_pass)
        sleep_condition(20,30)
        click_action(driver, css_selector=".lm-actions > button:nth-child(2)").click()
        sleep_condition(20,30)
    except:
        pass