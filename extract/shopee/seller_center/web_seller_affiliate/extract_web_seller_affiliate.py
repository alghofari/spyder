import time

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By


def get_data(driver: WebDriver):
    # go to affiliator page
    driver.get("https://seller.shopee.co.id/portal/web-seller-affiliate/conversion_report")
    time.sleep(10)

    # click export button
    export_button_path = """//*[@id="root"]/div[1]/div[2]/div[2]/div/div/div[2]/div/div/div/div[1]/div[2]/button"""
    driver.find_element(By.XPATH, export_button_path).click()
    time.sleep(10)

    # go to affiliator export page
    driver.get("https://seller.shopee.co.id/portal/web-seller-affiliate/export")
    time.sleep(10)

    # click download button
    download_button_path = """//*[@id="root"]/div[1]/div[2]/div[2]/div/div/div[2]/div/div/div[3]/div[1]/div/div/table/tbody/tr[1]/td[4]/div/button"""
    driver.find_element(By.XPATH, download_button_path).click()
    time.sleep(10)


def main(driver):
    get_data(driver)
