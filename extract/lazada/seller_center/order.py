import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


def download_order(driver):
    wait = WebDriverWait(driver, 10)

    # go to order page
    url = f"https://sellercenter.lazada.co.id/apps/order/list?oldVersion=1&status=all"
    driver.get(url)
    time.sleep(15)

    # click date filter 30 hari terakhir
    driver.find_element(By.XPATH, '//span[text()="30 hari terakhir"]').click()
    time.sleep(5)

    # click export button
    driver.find_element(By.XPATH, '//span[text()="Export"]').click()
    time.sleep(5)

    # click export all button
    driver.find_element(By.XPATH, '//div[text()="Export All"]').click()
    time.sleep(5)

    # click ok confirm export button
    ok_confirm_export_button = '//button[@class="next-btn next-medium next-btn-primary next-dialog-btn"]'
    wait.until(ec.visibility_of_element_located((By.XPATH, ok_confirm_export_button)))
    wait.until(ec.element_to_be_clickable((By.XPATH, ok_confirm_export_button))).click()
    time.sleep(180)


def main(driver, store_name):
    try:
        download_order(driver)
    except Exception as e:
        print(f"not able to collect order data from {store_name}")
        print(e)
        pass
