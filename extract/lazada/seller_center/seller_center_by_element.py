import json
import os
import pathlib

from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait
from helpers.date_helper import get_today

from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import set_webdriver


def driver_init(result_directory):
    preference = {
        "credentials_enable_service": False,
        "download.default_directory": result_directory,
        "download.prompt_for_download": False,
        "profile.password_manager_enabled": False
    }

    return set_webdriver(additional_opts=None, exp_opt_pref=preference, driver_type='webdriver')


def login(driver, url, email, password):
    # Go to login page
    driver.get(url=url)
    wait = WebDriverWait(driver, 7)

    # Fill email section
    sleep_condition(7, 17)
    wait.until(ec.visibility_of_element_located((By.XPATH, '//input[@id="account"]'))) # Wait email section appear
    sleep_condition(7, 17)
    wait.until(ec.element_to_be_clickable((By.XPATH, '//input[@id="account"]'))) # Wait email section clickable
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//input[@id="account"]').click() # Click the email section
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//input[@id="account"]').send_keys(email) # Send the email
    sleep_condition(7, 17)
    print("Email filled")

    # Fill password section
    sleep_condition(7, 17)
    wait.until(ec.visibility_of_element_located((By.XPATH, '//input[@id="password"]'))) # Wait password section appear
    sleep_condition(7, 17)
    wait.until(ec.element_to_be_clickable((By.XPATH, '//input[@id="password"]'))) # Wait password section clickable
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//input[@id="password"]').click() # Click the password section
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//input[@id="password"]').send_keys(password) # Send the password
    sleep_condition(7, 17)
    print("Password filled")

    # Click login section 
    wait.until(ec.element_to_be_clickable((By.XPATH, '//button[@type="submit"]'))).click()
    sleep_condition(7, 17)


def redirect_to_all_order_list_page(driver, url):
    driver.get(url)
    sleep_condition(7, 17)


def download_order_data_by_month(driver):
    wait =WebDriverWait(driver, 7)

    sleep_condition(7, 17)
    wait.until(ec.visibility_of_element_located((By.XPATH, '//span[text()="7 hari terakhir"]'))) # Wait password section appear
    sleep_condition(7, 17)
    wait.until(ec.element_to_be_clickable((By.XPATH, '//span[text()="7 hari terakhir"]')))
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//span[text()="7 hari terakhir"]').click()
    sleep_condition(7, 17)

    driver.find_element(By.XPATH, '//span[text()="Export"]').click()
    sleep_condition(7, 17)
    wait.until(ec.visibility_of_element_located((By.XPATH, '//div[text()="Export All"]'))) # Wait password section appear
    sleep_condition(7, 17)
    wait.until(ec.element_to_be_clickable((By.XPATH, '//div[text()="Export All"]')))
    sleep_condition(7, 17)
    driver.find_element(By.XPATH, '//div[text()="Export All"]').click()

    sleep_condition(7, 17)
    wait.until(ec.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[2]/div/div/div[2]/button[2]/span')))
    driver.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/div/div[2]/button[2]/span').click()

    sleep_condition(200, 600)


def main(marketplace):
    config_path = os.environ['SELLER_CENTER_CONFIG']
    config_json = open(config_path)
    config = json.load(config_json)

    all_order_list_url = 'https://sellercenter.lazada.co.id/apps/order/list?oldVersion=1&status=all'

    display = Display(size=(1920, 1080))
    display.start()

    base_path = f"assets/excel/{marketplace}/order/{config['os_name']}/{get_today()}"

    result_directory = f"{os.getcwd()}/{base_path}"

    print(f"Result directory: {result_directory}")
    pathlib.Path(result_directory).mkdir(parents=True, exist_ok=True)

    driver = driver_init(result_directory=result_directory)

    login(driver=driver, url=config['login_url'], email=config['os_email'], password=config['os_password'])
    sleep_condition(7,17)
    redirect_to_all_order_list_page(driver=driver, url=all_order_list_url)
    sleep_condition(7,17)
    download_order_data_by_month(driver=driver)

    driver.quit()
    display.stop()
